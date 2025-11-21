# main.py
import asyncio
import csv
import logging
import re
from dataclasses import dataclass, fields, astuple
from typing import Final
import httpx

# TODO: 去除“立绘后缀"
# TODO: 去除两个相同的skin_name

# --- 1. 配置模块 ---

# 可配置的常量
CHAR_API_BASE_URL: Final[str] = "https://api.kivo.wiki/api/v1/data/students/{student_id}"
SPINE_API_BASE_URL: Final[str] = "https://api.kivo.wiki/api/v1/data/spines/{spine_id}"

FINAL_STUDENT_ID: Final[int] = 566
STUDENT_ID_RANGE: Final[range] = range(1, FINAL_STUDENT_ID + 1)

OUTPUT_FILENAME: Final[str] = "students_data.csv"
SKIPPED_FILENAME: Final[str] = "skipped_ids.csv"

MAX_CONCURRENT_REQUESTS: Final[int] = 3
REQUEST_DELAY_SECONDS: Final[float] = 2

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# 设置 httpx 日志级别为 WARNING，以屏蔽 INFO 级别的成功请求日志
logging.getLogger("httpx").setLevel(logging.WARNING)


# --- 2. 数据结构定义 ---

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


class APIClient:
    """负责处理所有网络请求的客户端"""

    def __init__(self, client: httpx.AsyncClient):
        self.client = client

        self.client.headers.update({
            "User-Agent": "BA-characters-internal-id (https://github.com/Agent-0808/BA-characters-internal-id)"
        })

    async def fetch_student_data(self, student_id: int) -> tuple[dict | None, str | None]:
        """
        根据学生ID获取数据。
        返回 (数据, None) 或 (None, 错误/跳过原因)。
        """
        url = CHAR_API_BASE_URL.format(student_id=student_id)
        try:
            response = await self.client.get(url, timeout=10.0)
            if response.status_code == 404:
                return None, "未找到 (404)"
            response.raise_for_status()
            return response.json(), None
        except httpx.RequestError as e:
            return None, f"网络错误: {e}"
        except Exception as e:
            logging.error(f"处理 ID {student_id} 时发生未知错误: {e}")
            return None, f"未知错误: {e}"

    async def fetch_spine_data(self, spine_id: int) -> tuple[dict[str, any] | None, str | None]:
        """根据 spine_id 获取 spine 数据。返回 (数据, None) 或 (None, 错误原因)"""
        url = SPINE_API_BASE_URL.format(spine_id=spine_id)
        try:
            response = await self.client.get(url, timeout=10.0)
            response.raise_for_status()
            json_response = response.json()
            # 确保返回的数据是有效的字典且包含 'data' 键
            if isinstance(json_response, dict) and 'data' in json_response:
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

# --- 4. 数据解析模块 ---

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

    def _get_spine_skip_reason(self, spine_item: dict[str, any]) -> str | None:
        """
        检查单个 spine 数据，如果应跳过则返回原因，否则返回 None。
        """
        if not spine_item or not spine_item.get("name"):
            return "缺少名称或数据无效"

        # 只接受spr类型
        if type := spine_item.get("type"):
            if type != "spr":
                return f"类型 ({type})"

        # 跳过特定后缀的形态
        SPINE_SUFFIXES_TO_SKIP: Final[list[str]] = ["_cn", "_steam", "_glitch_spr", "_cbt"]

        for suffix in SPINE_SUFFIXES_TO_SKIP:
            if spine_item.get("name").lower().endswith(suffix.lower()):
                return f"后缀 ({suffix})"

        return None

    def _validate_and_get_skip_reason(self, char_data: dict | None) -> str | None:
        """
        对JSON数据进行预检查，如果应跳过则返回原因，否则返回None。
        """
        if not char_data or 'data' not in char_data:
            return "数据无效或缺少 'data' 键"

        data = char_data['data']
        if not data:
            return "键 'data' 的值为空"

        # 规则1: 跳过特定学校ID（例如官方账号）
        if data.get("school") == 30:
            return "官方账号"

        return None

    def _build_name(self, family: str | None, given: str | None) -> str:
        """根据姓和名构建全名"""
        family_name = family or ""
        given_name = given or ""
        if family_name:
            return f"{family_name} {given_name}".strip()
        return given_name

    def _normalize_file_id(self, file_id: str) -> str:
        """
        标准化文件ID格式：
        - 移除 'J_' 前缀
        - 移除 '_spr' 后缀
        - CH/NP 类统一使用大写
        - 其他类统一使用小写
        """
        # 移除 'J_' 前缀
        if file_id.startswith('J_'):
            file_id = file_id.removeprefix('J_')

        # 移除 '_spr' 后缀
        if file_id.endswith('_spr'):
            file_id = file_id[:-4]  # 移除 "_spr"

        # 检查是否以CH或NP开头，并且后面跟着4个数字
        if re.match(r"^(CH|NP)\d{4}$", file_id, re.IGNORECASE):
            return file_id.upper()
        return file_id.lower()

    def _process_spine_remark(self, remark: str | None, base_skin: str | None) -> str:
        """
        处理 Spine 备注信息
        """
        if not remark:
            return ""

        # "初始立绘" 直接忽略
        if remark == "初始立绘": return ""

        # 去除后缀
        suffixes = ["立绘", "差分"]
        for suffix in suffixes:
            processed = remark.removesuffix(suffix)

        # 如果处理后的备注与该角色的基础皮肤名一致，则不重复添加
        if base_skin and processed == base_skin:
            return ""

        return processed

    def _build_formatted_name(
        self, 
        data: dict, 
        lang_key: str, 
        spine_remark: str
    ) -> str:
        """
        通用方法：根据语言配置构建最终名称（姓名 + 皮肤后缀）
        """
        fam_key, giv_key, skin_key, include_skin = self._LANG_CONFIG[lang_key]
        
        # 1. 构建基础姓名
        base_name = self._build_name(data.get(fam_key), data.get(giv_key))
        
        # 如果连名字都没有（比如CN名字为空），直接返回空字符串
        if not base_name:
            return ""

        # 2. 如果该语言不需要皮肤（如EN/KR），直接返回姓名
        if not include_skin:
            return base_name

        # 3. 处理皮肤名称
        base_skin = data.get(skin_key) or ""
        
        # 调用统一的处理函数处理 spine_remark
        processed_remark = self._process_spine_remark(spine_remark, base_skin)
        
        skin_parts = []
        if base_skin:
            skin_parts.append(base_skin)
        
        if processed_remark:
            skin_parts.append(processed_remark)
        
        final_skin = ",".join(skin_parts)

        if final_skin:
            return f"{base_name}（{final_skin}）"
        return base_name

    def parse(self, json_data: dict, kivo_wiki_id: int, spine_data: list[dict[str, any]]) -> tuple[
        list[StudentForm], list[SkippedRecord], str | None]:
        """
        解析单个JSON响应。
        返回 (StudentForm列表, SkippedRecord列表, 学生级别的跳过原因 | None)。
        """
        if skip_reason := self._validate_and_get_skip_reason(json_data):
            return [], [], skip_reason

        data = json_data['data']
        results: list[StudentForm] = []
        skipped_spines: list[SkippedRecord] = []
        processed_file_ids: set[str] = set()

        # 预先获取用于 SkippedRecord 的基础信息 (使用 default/jp 逻辑)
        base_name_jp = self._build_name(data.get("family_name_jp"), data.get("given_name_jp"))
        base_name_en = self._build_name(data.get("family_name_en"), data.get("given_name_en"))
        # 默认 name 用于记录
        default_name = self._build_name(data.get("family_name"), data.get("given_name"))

        # 从 spine 数据提取 file_id
        for spine_item in spine_data:
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
            file_id = self._normalize_file_id(spine_name_raw)
            
            if not file_id or file_id in processed_file_ids:
                continue

            # 获取 Spine 备注
            spine_id = spine_item.get("id")
            spine_remark = spine_item.get("remark", "")

            # --- 构建各语言名称 ---
            # 使用字典推导式一次性生成所有需要的名称字段
            # map key (e.g., 'cn') -> formatted name string
            names = {
                key: self._build_formatted_name(data, key, spine_remark)
                for key in self._LANG_CONFIG
            }

            # 计算 skin_name (仅用于 skin_name 字段，逻辑同 default 但只取括号内部分)
            # 这里复用一下逻辑，手动构建
            base_skin = data.get("skin") or ""
            # 调用统一的处理函数处理 spine_remark
            processed_remark = self._process_spine_remark(spine_remark, base_skin)

            skin_parts = []
            if base_skin:
                skin_parts.append(base_skin)
            if processed_remark:
                skin_parts.append(processed_remark)
                
            final_skin_str = ",".join(skin_parts)

            results.append(StudentForm(
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
            ))
            processed_file_ids.add(file_id)

        if not results and not skipped_spines:
            return [], [], "未找到可解析的角色形态"

        return results, skipped_spines, None

# --- 5. 文件输出模块 ---

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


# --- 6. 主逻辑与执行 ---

async def process_student_id(
    student_id: int,
    client: APIClient,
    parser: DataParser,
    semaphore: asyncio.Semaphore
) -> tuple[int, list[StudentForm], list[SkippedRecord]]:
    """
    获取、解析并处理单个学生ID的数据。
    返回学生ID、处理结果的列表和一个SkippedRecord列表。
    """
    async with semaphore:
        all_skipped: list[SkippedRecord] = []
        json_data, fetch_reason = await client.fetch_student_data(student_id)
        # 即使请求学生数据失败，也需要延迟，避免对API造成过大压力
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
    all_student_forms: list[StudentForm] = []
    skipped_records: list[SkippedRecord] = []

    async with httpx.AsyncClient() as http_client:
        client = APIClient(http_client)
        student_ids = list(STUDENT_ID_RANGE)
        total_count = len(student_ids)

        tasks = [
            process_student_id(student_id, client, parser, semaphore)
            for student_id in student_ids
        ]

        logging.info(f"开始处理 {total_count} 个学生 ID...")

        processed_count = 0
        for future in asyncio.as_completed(tasks):
            processed_count += 1
            student_id, forms_list, newly_skipped_records = await future

            progress_prefix = f"[{processed_count}/{total_count}]"

            if forms_list:
                # 成功提取到数据
                file_ids_str = ", ".join(form.file_id for form in forms_list)
                print(f"{progress_prefix} ID: {student_id} -> 成功, File IDs: {file_ids_str}")
                all_student_forms.extend(forms_list)

            if newly_skipped_records:
                # 记录并打印跳过信息
                for skipped in newly_skipped_records:
                    if skipped.spine_id:
                        print(f"{progress_prefix} ID: {student_id} -> Spine ID {skipped.spine_id} 已跳过 ({skipped.reason})")
                    else:
                        print(f"{progress_prefix} ID: {student_id} -> 已跳过 ({skipped.reason})")
                skipped_records.extend(newly_skipped_records)


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