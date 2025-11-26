import asyncio
import csv
import logging
import re
import json
from pathlib import Path
from dataclasses import dataclass, fields, astuple
from typing import Final, Any
import httpx

# TODO: 去除“立绘后缀"
# TODO: 去除两个相同的skin_name

# --- 配置模块 ---

# 可配置的常量
CHAR_API_BASE_URL: Final[str] = "https://api.kivo.wiki/api/v1/data/students/{student_id}"
SPINE_API_BASE_URL: Final[str] = "https://api.kivo.wiki/api/v1/data/spines/{spine_id}"

FINAL_STUDENT_ID: Final[int] = 566
STUDENT_ID_RANGE: Final[range] = range(1, FINAL_STUDENT_ID + 1)

OUTPUT_FILENAME: Final[str] = "students_data.csv"
SKIPPED_FILENAME: Final[str] = "skipped_ids.csv"
CACHE_DIR: Final[Path] = Path("cache")

MAX_CONCURRENT_REQUESTS: Final[int] = 3
REQUEST_DELAY_SECONDS: Final[float] = 2

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# 设置 httpx 日志级别为 WARNING，以屏蔽 INFO 级别的成功请求日志
logging.getLogger("httpx").setLevel(logging.WARNING)


# --- 数据结构定义 ---

@dataclass
class StudentForm:
    """用于存储单个角色形态结构化数据的类"""
    file_id: str
    char_id: int
    spine_id: int | None
    full_name: str
    name: str
    skin_name: str
    name_cn: str
    name_jp: str
    name_tw: str
    name_en: str
    name_kr: str


@dataclass
class SkippedRecord:
    """用于存储跳过的ID及其原因的类"""
    student_id: int = 0
    spine_id: int | None = None
    reason: str = ""
    spine_name: str | None = None
    spine_remark: str | None = None
    name: str = ""
    name_jp: str = ""
    name_en: str = ""
    school: int | str = ""

class CacheManager:
    """负责本地数据的缓存管理"""

    def __init__(self, base_dir: Path = CACHE_DIR):
        self.base_dir = base_dir
        self.students_dir = base_dir / "students"
        self.spines_dir = base_dir / "spines"
        self._ensure_dirs()

    def _ensure_dirs(self):
        """确保缓存目录存在"""
        self.students_dir.mkdir(parents=True, exist_ok=True)
        self.spines_dir.mkdir(parents=True, exist_ok=True)

    def _clean_student_data(self, json_data: dict[str, Any]) -> dict[str, Any]:
        """
        清洗学生数据，移除不需要的大文本字段以节省空间
        """
        if not json_data or 'data' not in json_data:
            return json_data

        data = json_data['data']
        if not isinstance(data, dict):
            return json_data

        # 1. 移除明确不需要的字段
        for field in ['more', 'gallery']:
            data.pop(field, None)

        # 2. 处理 Voice 字段，仅保留“是否为空列表”的信息
        # 逻辑：如果有内容，替换为占位符表示存在；如果为空或不存在，保持为空列表
        voice_fields = ['voice', 'voice_cn', 'voice_kr']
        for field in voice_fields:
            if field in data:
                content = data[field]
                if isinstance(content, list) and content:
                    # 如果列表不为空，替换为简短标记，保留"非空"这一信息
                    data[field] = ["(cached_stripped)"]
                else:
                    # 否则设置为空列表
                    data[field] = []

        return json_data

    async def get_student(self, student_id: int) -> dict | None:
        """从缓存读取学生数据"""
        file_path = self.students_dir / f"{student_id}.json"
        return await self._read_json(file_path)

    async def save_student(self, student_id: int, data: dict):
        """清洗并保存学生数据到缓存"""
        cleaned_data = self._clean_student_data(data)
        file_path = self.students_dir / f"{student_id}.json"
        await self._write_json(file_path, cleaned_data)

    async def get_spine(self, spine_id: int) -> dict | None:
        """从缓存读取 Spine 数据"""
        file_path = self.spines_dir / f"{spine_id}.json"
        return await self._read_json(file_path)

    async def save_spine(self, spine_id: int, data: dict):
        """保存 Spine 数据到缓存 (Spine 数据通常较小，不做额外清洗)"""
        file_path = self.spines_dir / f"{spine_id}.json"
        await self._write_json(file_path, data)

    async def _read_json(self, path: Path) -> dict | None:
        """异步读取 JSON 文件"""
        if not path.exists():
            return None
        try:
            # 使用 asyncio.to_thread 避免文件IO阻塞事件循环
            return await asyncio.to_thread(self._read_json_sync, path)
        except Exception as e:
            logging.warning(f"读取缓存失败 {path}: {e}")
            return None

    def _read_json_sync(self, path: Path) -> dict:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    async def _write_json(self, path: Path, data: dict):
        """异步写入紧凑格式 JSON"""
        try:
            await asyncio.to_thread(self._write_json_sync, path, data)
        except Exception as e:
            logging.error(f"写入缓存失败 {path}: {e}")

    def _write_json_sync(self, path: Path, data: dict):
        with open(path, 'w', encoding='utf-8') as f:
            # 使用 separators 生成紧凑的 JSON (无多余空格)
            json.dump(data, f, ensure_ascii=False, separators=(',', ':'))

class APIClient:
    """负责处理所有网络请求及缓存管理的客户端"""

    def __init__(self, client: httpx.AsyncClient, cache_manager: CacheManager):
        self.client = client
        self.cache = cache_manager
        
        # 统计 API 请求次数
        self.student_req_count: int = 0
        self.spine_req_count: int = 0

        self.client.headers.update({
            "User-Agent": "BA-characters-internal-id (https://github.com/Agent-0808/BA-characters-internal-id)"
        })

    async def fetch_student_data(self, student_id: int) -> tuple[dict | None, str | None, bool]:
        """
        根据学生ID获取数据（优先查缓存）。
        返回 (数据, 错误/跳过原因, 是否命中缓存)。
        """
        # 1. 尝试从缓存获取
        if cached_data := await self.cache.get_student(student_id):
            logging.debug(f"ID {student_id}: 命中缓存")
            # 返回 True 表示命中缓存
            return cached_data, None, True

        # 2. 缓存未命中，从 API 获取
        # 记录请求计数
        self.student_req_count += 1
        
        url = CHAR_API_BASE_URL.format(student_id=student_id)
        try:
            response = await self.client.get(url, timeout=10.0)
            if response.status_code == 404:
                # 未找到，未命中缓存
                return None, "未找到 (404)", False
            response.raise_for_status()
            
            json_data = response.json()
            
            # 3. 成功获取后，保存到缓存
            if json_data and json_data.get('code') == 2000:
                await self.cache.save_student(student_id, json_data)
            
            # 返回 False 表示来自 API 请求
            return json_data, None, False

        except httpx.RequestError as e:
            return None, f"网络错误: {e}", False
        except Exception as e:
            logging.error(f"处理 ID {student_id} 时发生未知错误: {e}")
            return None, f"未知错误: {e}", False

    async def fetch_spine_data(self, spine_id: int) -> tuple[dict[str, Any] | None, str | None]:
        """
        根据 spine_id 获取 spine 数据（优先查缓存）。
        注意：此函数暂不需要返回是否命中缓存，因为并发获取时不由它控制主延迟。
        """
        # 1. 尝试从缓存获取
        if cached_data := await self.cache.get_spine(spine_id):
            if isinstance(cached_data, dict) and 'data' in cached_data:
                return cached_data['data'], None
            return cached_data, None 

        # 2. 缓存未命中，从 API 获取
        # 记录请求计数
        self.spine_req_count += 1

        url = SPINE_API_BASE_URL.format(spine_id=spine_id)
        try:
            response = await self.client.get(url, timeout=10.0)
            response.raise_for_status()
            json_response = response.json()
            
            if isinstance(json_response, dict) and 'data' in json_response:
                # 3. 成功获取后，保存完整响应到缓存
                await self.cache.save_spine(spine_id, json_response)
                return json_response['data'], None
                
            logging.warning(f"Spine ID {spine_id} 的响应格式无效: {json_response}")
            return None, "响应格式无效"
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None, "未找到 (404)"
            return None, f"HTTP错误: {e.response.status_code}"
        except httpx.RequestError as e:
            logging.warning(f"请求 Spine ID {spine_id} 时网络错误: {e}")
            return None, f"网络错误: {e}"
        except Exception as e:
            logging.error(f"处理 Spine ID {spine_id} 时发生未知错误: {e}")
            return None, f"未知错误: {e}"

# --- 数据解析模块 ---

class DataParser:
    """负责解析JSON数据并根据规则提取信息"""

    # 语言配置映射：(语言后缀, 是否包含皮肤名称)
    # key: 目标字段后缀, value: (JSON中的姓key, JSON中的名key, JSON中的皮肤key, 是否附加皮肤)
    _LANG_CONFIG: Final[dict[str, tuple[str, str, str, bool]]] = {
        "full_name": ("family_name", "given_name", "skin", True), # 包含皮肤的完整名称
        "name": ("family_name", "given_name", "", False), # 不包含皮肤的基础名称
        "cn": ("family_name_cn", "given_name_cn", "skin_cn", True),
        "jp": ("family_name_jp", "given_name_jp", "skin_jp", True),
        "tw": ("family_name_zh_tw", "given_name_zh_tw", "skin_zh_tw", True),
        "en": ("family_name_en", "given_name_en", "", False), # EN 不包含皮肤
        "kr": ("family_name_kr", "given_name_kr", "", False), # KR 不包含皮肤
    }

    # Student ID / 整体数据层面的处理 (最顶层验证与基础工具)

    def _validate_and_get_skip_reason(self, char_data: dict | None) -> str | None:
        """对JSON数据进行预检查（检查 ID、School 等整体属性）"""
        if not char_data or 'data' not in char_data:
            return "数据无效或缺少 'data' 键"

        data = char_data['data']
        if not data:
            return "键 'data' 的值为空"

        # 跳过特定学校ID（例如官方账号）
        if data.get("school") == 30:
            return "官方账号"

        # 遁
        if data.get("id") == 348:
            return "彩蛋"

        return None

    def _build_name(self, family: str | None, given: str | None) -> str:
        """基础工具：根据姓和名构建全名"""
        family_name = family or ""
        given_name = given or ""
        if family_name:
            return f"{family_name} {given_name}".strip()
        return given_name

    # File ID 层面的处理 (标识符标准化)

    def _normalize_file_id(self, file_id: str) -> str:
        """
        标准化文件ID格式：
        - 优先提取标准的 CHxxxx / NPxxxx 格式
        - 移除 new_, old_ 前缀和 _spr 等后缀
        """
        # 1. 尝试直接提取标准格式 (CH/NP + 4位数字)
        if match := re.search(r"(CH|NP)\d{4}", file_id, re.IGNORECASE):
            return match.group(0).upper()

        # 2. 否则手动清洗
        cleaned_id = file_id.strip()
        for prefix in ['J_', 'new_', 'old_']:
            if cleaned_id.lower().startswith(prefix.lower()):
                cleaned_id = cleaned_id[len(prefix):]

        for suffix in ['_spr', '_spr_update']:
            if cleaned_id.endswith(suffix):
                cleaned_id = cleaned_id.removesuffix(suffix)
            
        return cleaned_id.lower()

    # Spine ID / 皮肤层面的处理 (跳过判断、正则清洗、名称组装)

    def _get_spine_skip_reason(self, spine_item: dict[str, Any]) -> str | None:
        """
        检查单个 spine 数据，如果应跳过则返回原因，否则返回 None。
        """
        if not spine_item or not (name := spine_item.get("name")):
            return "缺少名称或数据无效"

        name_lower = name.lower()

        # 只接受spr类型
        ACCEPT_TYPES = ["spr"]
        if (type_ := spine_item.get("type")) not in ACCEPT_TYPES:
            return f"类型 ({type_})"
        
        # 跳过包含特定关键词的形态
        SPINE_KEYWORDS_TO_SKIP: Final[list[str]] = ["toschool", "minori", "ui_raidboss"]
        for keyword in SPINE_KEYWORDS_TO_SKIP:
            if keyword in name_lower:
                return f"包含 ({keyword})"

        # 跳过特定后缀的形态
        SPINE_SUFFIXES_TO_SKIP: Final[list[str]] = [
            "_cn", "_steam", "_glitch_spr", "_cbt", "_halofix", "spr-2", "_old"
        ]
        for suffix in SPINE_SUFFIXES_TO_SKIP:
            if name_lower.endswith(suffix):
                return f"后缀 ({suffix.removeprefix('_')})"

        return None

    def _process_spine_remark(self, remark: str | None, base_skin: str | None, name: str | None = None) -> str:
        """
        处理 Spine 备注信息（核心正则清洗逻辑）
        """
        if not remark:
            return ""

        processed = remark

        # 正则清洗规则列表
        patterns = [
            r"初始立绘",
            r"立绘",
            r"差分",
            
            # 强力清除：只要括号里包含类似年份或日期的数字结构，直接删掉整个括号
            # 匹配：括号 -> 非括号内容 -> 2到4位数字接"年"或"." -> 非括号内容 -> 括号
            # 这能搞定 (23.11.08之前), (2023年1月前)
            r"[\(（][^\)）]*?\d{2,4}[年\.][^\)）]*?[\)）]",

            # 清除裸露的日期串，并强制匹配后面的方位词
            # 匹配：数字 -> 年/. -> 数字 -> [月/.] -> [日] -> [空格] -> [之前/之后/前/后/更新/版本修改]
            r"\d{2,4}[年\.-]\d{1,2}[月\.-]\d{0,2}日?\s*(?:之?[前后]|更新|版本修改)?",

            # 清除特定的状态词
            r"[\(（](?:已)?更新至实装[\)）]",
            r"修正版?",
            r"更新",
            r"(?i)\b(old|new|fixed|ver\.?\d*)\b",

            # 删除 "旧" 和 "新"
            r"[旧新]",

            # 删除空括号
            r"[\(（][\)）]",
        ]

        for pat in patterns:
            processed = re.sub(pat, "", processed)

        # 后处理：清理因删除单词留下的标点符号
        processed = processed.replace("()", "").replace("（）", "").strip()
        
        # 移除开头和结尾的逗号/空格
        processed = processed.strip(",， ")
        # 移除中间可能出现的双逗号
        processed = re.sub(r"[,，]\s*[,，]", ",", processed)
        # 括号改为逗号分隔，例如"冬装（无围巾）"→"冬装,无围巾"
        processed = re.sub(r"[\(（]\s*([^)）]+?)\s*[\)）]", r",\1", processed)
        processed = processed.strip(",，")
        
        # 特定替换规则列表
        replacement_rules = [
            (r"礼服(?:日奈|亚子)", "礼服"),
            ("西服", "西装"),
        ]
        
        # 应用替换规则
        for pattern, replacement in replacement_rules:
            processed = re.sub(pattern, replacement, processed)

        # 如果处理后的备注与该角色的基础皮肤名一致，则不重复添加
        if base_skin and processed == base_skin:
            return ""
        # 如果处理后的备注与角色名相同，也不添加
        if name and processed == name:
            return ""

        return processed

    def _build_formatted_name(self, data: dict, lang_key: str, spine_remark: str) -> str:
        """根据语言配置构建最终名称"""
        fam_key, giv_key, skin_key, include_skin = self._LANG_CONFIG[lang_key]
        
        # 构建基础姓名
        base_name = self._build_name(data.get(fam_key), data.get(giv_key))
        
        # 如果连名字都没有（比如CN名字为空），直接返回空字符串
        if not base_name:
            return ""

        # 如果该语言不需要皮肤（如EN/KR），直接返回姓名
        if not include_skin:
            return base_name

        # 处理皮肤名称
        base_skin = data.get(skin_key) or ""
        processed_remark = self._process_spine_remark(spine_remark, base_skin, base_name)
        
        skin_parts = []
        if base_skin:
            skin_parts.append(base_skin)
        if processed_remark:
            skin_parts.append(processed_remark)
        
        final_skin = ",".join(skin_parts)

        if final_skin:
            return f"{base_name}（{final_skin}）"
        return base_name

    # Entry Point

    def parse(self, json_data: dict, kivo_wiki_id: int, spine_data: list[dict[str, Any]]) -> tuple[
        list[StudentForm], list[SkippedRecord], str | None]:
        """
        主解析入口：
        1. 验证 Student 数据
        2. 遍历 Spine 数据
        3. 清洗 File ID 和 Spine Remark
        4. 生成最终结果
        """
        if skip_reason := self._validate_and_get_skip_reason(json_data):
            return [], [], skip_reason

        data = json_data['data']
        # 使用字典去重，key为标准化后的file_id
        forms_map: dict[str, StudentForm] = {}
        skipped_spines: list[SkippedRecord] = []

        # 预先计算基础名称，用于 SkippedRecord
        base_name_jp = self._build_name(data.get("family_name_jp"), data.get("given_name_jp"))
        base_name_en = self._build_name(data.get("family_name_en"), data.get("given_name_en"))
        default_name = self._build_name(data.get("family_name"), data.get("given_name"))

        for spine_item in spine_data:
            # 3.1 检查 Spine 是否跳过
            if skip_reason := self._get_spine_skip_reason(spine_item):
                skipped_spines.append(SkippedRecord(
                    student_id=kivo_wiki_id,
                    spine_id=spine_item.get("id"),
                    reason=skip_reason,
                    spine_name=spine_item.get("name"),
                    spine_remark=spine_item.get("remark", ""),
                    name=default_name, 
                    name_jp=base_name_jp, 
                    name_en=base_name_en, 
                    school=data.get("school", "")
                ))
                continue

            spine_name_raw = spine_item["name"]
            # 处理 File ID
            file_id = self._normalize_file_id(spine_name_raw)
            
            if not file_id:
                continue

            # 获取 Spine 备注
            spine_id = spine_item.get("id")
            spine_remark = spine_item.get("remark", "")

            # 3.3 处理各种语言的名称 (内部调用 _process_spine_remark)
            names = {
                key: self._build_formatted_name(data, key, spine_remark)
                for key in self._LANG_CONFIG
            }

            # 单独计算 skin_name 字段
            base_skin = data.get("skin") or ""
            processed_remark = self._process_spine_remark(spine_remark, base_skin, default_name)
            # 使用推导式构建列表，自动过滤空字符串
            skin_parts = [s for s in [base_skin, processed_remark] if s]
            final_skin_str = ",".join(skin_parts)

            form = StudentForm(
                file_id=file_id,
                char_id=kivo_wiki_id,
                spine_id=spine_id,
                full_name=names["full_name"],
                name=names["name"],
                skin_name=final_skin_str,
                name_cn=names["cn"],
                name_jp=names["jp"],
                name_tw=names["tw"],
                name_en=names["en"],
                name_kr=names["kr"]
            )

            # --- 去重与合并逻辑 ---
            if file_id in forms_map:
                existing_form = forms_map[file_id]
                # 简单的“后者优先”策略：假设 spine_id 越大代表版本越新
                # 这样新版（ID大）会覆盖旧版（ID小）
                if (spine_id or 0) > (existing_form.spine_id or 0):
                    forms_map[file_id] = form
            else:
                forms_map[file_id] = form

        results = list(forms_map.values())

        if not results and not skipped_spines:
            return [], [], "未找到可解析的角色形态"

        return results, skipped_spines, None

# --- 文件输出模块 ---

class CsvWriter:
    """负责将处理好的数据写入CSV文件"""

    def __init__(self, filename: str):
        self.filename = filename

    def _get_alternative_filename(self, original_filename: str) -> str:
        """生成备用文件名"""
        base, ext = original_filename.rsplit('.', 1)
        return f"{base}_backup.{ext}"

    def write(self, data: list[StudentForm]):
        """将StudentForm列表写入CSV文件"""
        if not data:
            logging.warning("没有可供写入的数据。")
            return

        filenames_to_try = [self.filename, self._get_alternative_filename(self.filename)]

        for filename in filenames_to_try:
            try:
                logging.info(f"开始将 {len(data)} 条记录写入到 {filename}...")
                with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                    # 获取dataclass的字段名作为表头
                    header = [f.name for f in fields(StudentForm)]
                    writer = csv.writer(csvfile)
                    writer.writerow(header)
                    # 使用推导式和astuple提高写入效率
                    writer.writerows(astuple(form) for form in data)
                logging.info(f"数据成功写入 {filename}。")
                return  # 成功写入，退出函数
            except IOError as e:
                if filename == filenames_to_try[-1]:
                    # 已经是最后一个文件名，仍然失败
                    logging.error(f"写入文件 {filename} 时发生错误: {e}")
                    logging.error("所有尝试的文件名均失败，数据未能保存。")
                else:
                    # 还有备用文件名可以尝试
                    logging.warning(f"写入文件 {filename} 失败，可能是文件被占用，尝试使用备用文件名...")
                    continue

    def write_skipped(self, data: list[SkippedRecord]):
        """将SkippedRecord列表写入CSV文件"""
        if not data:
            logging.warning("没有可供写入的跳过记录。")
            return

        filenames_to_try = [self.filename, self._get_alternative_filename(self.filename)]

        for filename in filenames_to_try:
            try:
                logging.info(f"开始将 {len(data)} 条跳过记录写入到 {filename}...")
                with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                    # 获取dataclass的字段名作为表头
                    header = [f.name for f in fields(SkippedRecord)]
                    writer = csv.writer(csvfile)
                    writer.writerow(header)
                    # 使用推导式和astuple提高写入效率
                    writer.writerows(astuple(record) for record in data)
                logging.info(f"跳过记录成功写入 {filename}。")
                return  # 成功写入，退出函数
            except IOError as e:
                if filename == filenames_to_try[-1]:
                    # 已经是最后一个文件名，仍然失败
                    logging.error(f"写入文件 {filename} 时发生错误: {e}")
                    logging.error("所有尝试的文件名均失败，跳过记录未能保存。")
                else:
                    # 还有备用文件名可以尝试
                    logging.warning(f"写入文件 {filename} 失败，可能是文件被占用，尝试使用备用文件名...")
                    continue


# --- 主逻辑与执行 ---

async def process_student_id(
    student_id: int,
    client: APIClient,
    parser: DataParser,
    semaphore: asyncio.Semaphore
) -> tuple[int, list[StudentForm], list[SkippedRecord]]:
    """
    获取、解析并处理单个学生ID的数据。
    """
    async with semaphore:
        all_skipped: list[SkippedRecord] = []
        
        # 获取数据，并得知来源是否为缓存
        json_data, fetch_reason, from_cache = await client.fetch_student_data(student_id)
        
        # 如果数据不是来自缓存（即发起了网络请求），则执行延迟以礼貌对待 API
        if not from_cache:
            await asyncio.sleep(REQUEST_DELAY_SECONDS)

        if not json_data:
            # 在无法获取JSON数据时，创建一个包含基本信息的SkippedRecord
            skipped = SkippedRecord(
                student_id=student_id,
                spine_id=None,
                reason=fetch_reason or "未知网络原因",
                spine_name=None, 
                spine_remark=None,
                name="", 
                name_jp="", 
                name_en="", 
                school=""
            )
            return student_id, [], [skipped]

        # 获取 spine 数据
        spine_ids = json_data.get("data", {}).get("spine", [])
        spine_tasks = [client.fetch_spine_data(sid) for sid in spine_ids if isinstance(sid, int)]
        spine_results_raw = await asyncio.gather(*spine_tasks)
        # 只提取成功获取的数据部分，忽略错误信息
        spine_results = [data for data, error in spine_results_raw if data is not None]

        forms, skipped_spines, student_skip_reason = parser.parse(json_data, student_id, spine_results)
        all_skipped.extend(skipped_spines)

        if student_skip_reason:
            # 如果整个学生因规则被跳过，则从JSON数据中提取详细信息
            data = json_data.get("data", {})
            name = parser._build_name(data.get("family_name"), data.get("given_name")) or data.get("given_name_cn", "")
            name_jp = parser._build_name(data.get("family_name_jp"), data.get("given_name_jp")) or ""
            name_en = parser._build_name(data.get("family_name_en"), data.get("given_name_en")) or ""
            school = data.get("school", "")

            skipped = SkippedRecord(
                student_id=student_id,
                spine_id=None,
                reason=student_skip_reason,
                spine_name=None, 
                spine_remark=None,
                name=name,
                name_jp=name_jp,
                name_en=name_en,
                school=school
            )
            all_skipped.append(skipped)

        return student_id, forms, all_skipped

async def main():
    """主执行函数"""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    parser = DataParser()
    cache_manager = CacheManager()
    
    all_student_forms: list[StudentForm] = []
    skipped_records: list[SkippedRecord] = []

    async with httpx.AsyncClient() as http_client:
        # 将 cache_manager 注入 APIClient
        client = APIClient(http_client, cache_manager)
        student_ids = list(STUDENT_ID_RANGE)
        total_count = len(student_ids)

        tasks = [
            process_student_id(student_id, client, parser, semaphore)
            for student_id in student_ids
        ]

        logging.info(f"开始处理 {total_count} 个学生 ID (使用缓存路径: {CACHE_DIR})...")

        processed_count = 0
        for future in asyncio.as_completed(tasks):
            processed_count += 1
            student_id, forms_list, newly_skipped_records = await future

            progress_prefix = f"[{processed_count}/{total_count}]"

            if forms_list:
                # 成功提取到数据
                file_ids_str = ", ".join(form.file_id for form in forms_list)
                logging.info(f"{progress_prefix} ID: {student_id} -> 成功, File IDs: {file_ids_str}")
                all_student_forms.extend(forms_list)

            if newly_skipped_records:
                # 记录并打印跳过信息
                for skipped in newly_skipped_records:
                    if skipped.spine_id:
                        logging.info(f"{progress_prefix} ID: {student_id} -> Spine ID {skipped.spine_id} 已跳过 ({skipped.reason})")
                    else:
                        logging.info(f"{progress_prefix} ID: {student_id} -> 已跳过 ({skipped.reason})")
                skipped_records.extend(newly_skipped_records)
        
        # 输出统计信息
        logging.info("-" * 40)
        logging.info(f"学生数据请求: {client.student_req_count}")
        logging.info(f"Spine 数据请求: {client.spine_req_count}")


    # 按 file_id 排序以保证输出顺序稳定
    all_student_forms.sort(key=lambda x: (x.char_id, x.file_id))

    # 按 student_id 和 spine_id 排序以保证输出顺序稳定
    skipped_records.sort(key=lambda x: (x.student_id, x.spine_id or -1))

    # 写入文件
    writer = CsvWriter(OUTPUT_FILENAME)
    writer.write(all_student_forms)

    # 写入跳过记录文件
    skipped_writer = CsvWriter(SKIPPED_FILENAME)
    skipped_writer.write_skipped(skipped_records)


if __name__ == "__main__":
    asyncio.run(main())