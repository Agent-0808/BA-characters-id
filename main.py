import asyncio
import csv
import logging
import re
from dataclasses import dataclass, fields, astuple
from typing import Final
import httpx

# --- 1. 配置模块 ---

# 可配置的常量
API_BASE_URL: Final[str] = "https://api.kivo.wiki/api/v1/data/students/{student_id}"
STUDENT_ID_RANGE: Final[range] = range(1, 567)
OUTPUT_FILENAME: Final[str] = "students_data.csv"
SKIPPED_FILENAME: Final[str] = "skipped_ids.csv"
MAX_CONCURRENT_REQUESTS: Final[int] = 5
REQUEST_DELAY_SECONDS: Final[float] = 0.5

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# 设置 httpx 日志级别为 WARNING，以屏蔽 INFO 级别的成功请求日志
logging.getLogger("httpx").setLevel(logging.WARNING)

# 正则表达式预编译
# 统一的 file_id 模式，匹配如 CH0201, np0001 等
FILE_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"(?:CH|ch|NP|np)\d{4}")


# --- 2. 数据结构定义 ---

@dataclass
class StudentForm:
    """用于存储单个角色形态结构化数据的类"""
    file_id: str
    name: str
    skin_name: str
    kivo_wiki_id: int
    name_cn: str
    name_jp: str
    name_tw: str
    name_en: str
    name_kr: str


@dataclass
class SkippedRecord:
    """用于存储跳过的ID及其原因的类"""
    student_id: int
    reason: str


# --- 3. API 请求模块 ---

class APIClient:
    """负责处理所有网络请求的客户端"""

    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    async def fetch_student_data(self, student_id: int) -> tuple[dict | None, str | None]:
        """
        根据学生ID获取数据。
        返回 (数据, None) 或 (None, 错误/跳过原因)。
        """
        url = API_BASE_URL.format(student_id=student_id)
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


# --- 4. 数据解析模块 ---

class DataParser:
    """负责解析JSON数据并根据规则提取信息"""

    def _is_valid_data(self, json_data: dict | None) -> bool:
        """检查返回的JSON数据是否有效"""
        if not json_data or 'data' not in json_data:
            return False

        char_datas = json_data['data'].get('character_datas')
        # 记录必须包含 character_datas 字段且其类型为列表。
        # 原有的 dev_name 检查过于严格，会导致一些有效的 NPC 数据被跳过。
        if not isinstance(char_datas, list):
            return False
        return True

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
        - CH/NP 类统一使用大写
        - 其他类统一使用小写
        """
        if file_id.upper().startswith(('CH', 'NP')):
            return file_id.upper()
        return file_id.lower()

    def _find_file_id_from_voice(self, voices: list[dict]) -> str | None:
        """
        最高优先级：从语音数据的 description 字段提取 file_id。
        """
        for voice in voices:
            description = voice.get("description", "")
            if match := FILE_ID_PATTERN.search(description):
                # 使用 group(0) 获取完整匹配，然后标准化格式
                return self._normalize_file_id(match.group(0))
        return None

    def _find_special_forms_from_gallery(self, gallery: list[dict]) -> dict[str, str]:
        """
        次高优先级：从图库中提取特殊形态的 file_id 及其形态名称。
        主要针对 "领航服差分" 等未在 character_datas 中定义的形态。
        """
        special_forms = {}
        for gallery_item in gallery:
            title = gallery_item.get("title", "")
            # 目标是寻找 character_datas 中未定义的特殊形态
            if "差分" in title:
                skin_name = title.replace("差分", "").strip()
                for image_url in gallery_item.get("images", []):
                    if match := FILE_ID_PATTERN.search(image_url):
                        # 使用 group(0) 获取完整匹配，然后标准化格式
                        file_id = self._normalize_file_id(match.group(0))
                        special_forms[file_id] = skin_name
        return special_forms

    def parse(self, json_data: dict, kivo_wiki_id: int) -> tuple[list[StudentForm], str | None]:
        """
        解析单个JSON响应。
        返回 (StudentForm列表, None) 或 ([], 跳过原因)。
        """
        if not self._is_valid_data(json_data):
            return [], "数据无效或不符合要求"

        data = json_data['data']
        results: list[StudentForm] = []
        processed_file_ids: set[str] = set()

        # 提取皮肤名称
        # 'skin' 字段通常是中文的皮肤名，'skin_jp' 字段是日语皮肤名，作为后备
        skin_cn_val = data.get("skin") or data.get("skin_cn")
        skin_jp_val = data.get("skin_jp")
        skin_tw_val = data.get("skin_zh_tw")

        # 提取并构建基础名称
        name = self._build_name(data.get("family_name"), data.get("given_name"))
        base_name_cn = self._build_name(data.get("family_name_cn"), data.get("given_name_cn"))
        base_name_jp = self._build_name(data.get("family_name_jp"), data.get("given_name_jp"))
        base_name_tw = self._build_name(data.get("family_name_zh_tw"), data.get("given_name_zh_tw"))
        base_name_en = self._build_name(data.get("family_name_en"), data.get("given_name_en"))
        base_name_kr = self._build_name(data.get("family_name_kr"), data.get("given_name_kr"))

        # 如果有皮肤名称，则附加到对应语言的名称后
        name_cn = f"{base_name_cn} （{skin_cn_val}）" if base_name_cn and skin_cn_val else base_name_cn
        name_jp = f"{base_name_jp} （{skin_jp_val}）" if base_name_jp and skin_jp_val else base_name_jp
        name_tw = f"{base_name_tw} （{skin_tw_val}）" if base_name_tw and skin_tw_val else base_name_tw
        name_en = base_name_en  # 英文和韩文名保持不变
        name_kr = base_name_kr

        # 1. 处理 character_datas 中的常规形态
        file_id_from_voice = self._find_file_id_from_voice(data.get("voice", []))

        for char_data in data.get("character_datas", []):
            file_id: str | None = None
            dev_name = char_data.get("dev_name")
            if not dev_name:
                continue

            # 优先级 1: 语音
            if file_id_from_voice:
                file_id = file_id_from_voice
            # 优先级 3: dev_name 作为后备
            else:
                file_id = self._normalize_file_id(dev_name.removesuffix("_default"))
                logging.debug(f"ID {kivo_wiki_id}: 未能从语音中找到 file_id, "
                              f"回退'{dev_name}' -> '{file_id}'")

            if file_id in processed_file_ids:
                continue

            skin_name = skin_cn_val or ""

            results.append(StudentForm(
                file_id=file_id,
                name=name,
                skin_name=skin_name,
                kivo_wiki_id=kivo_wiki_id,
                name_cn=name_cn,
                name_jp=name_jp,
                name_tw=name_tw,
                name_en=name_en,
                name_kr=name_kr
            ))
            processed_file_ids.add(file_id)

        # 2. 处理 gallery 中的特殊形态
        special_forms = self._find_special_forms_from_gallery(data.get("gallery", []))
        for file_id, skin_name in special_forms.items():
            if file_id not in processed_file_ids:
                results.append(StudentForm(
                    file_id=file_id,
                    name=name,
                    skin_name=skin_name,
                    kivo_wiki_id=kivo_wiki_id,
                    name_cn=name_cn,
                    name_jp=name_jp,
                    name_tw=name_tw,
                    name_en=name_en,
                    name_kr=name_kr
                ))
                processed_file_ids.add(file_id)

        return results, None


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
) -> tuple[int, list[StudentForm], str | None]:
    """
    获取、解析并处理单个学生ID的数据。
    返回学生ID、处理结果的列表和可选的跳过原因。
    """
    async with semaphore:
        json_data, reason = await client.fetch_student_data(student_id)
        await asyncio.sleep(REQUEST_DELAY_SECONDS)  # 请求延迟
        if not json_data:
            return student_id, [], reason
        
        forms, reason = parser.parse(json_data, student_id)
        return student_id, forms, reason

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
            student_id, forms_list, skip_reason = await future

            progress_prefix = f"[{processed_count}/{total_count}]"

            if forms_list:
                # 成功提取到数据
                file_ids_str = ", ".join(form.file_id for form in forms_list)
                print(f"{progress_prefix} ID: {student_id} -> 成功, File IDs: {file_ids_str}")
                all_student_forms.extend(forms_list)
            else:
                # 失败或跳过，使用返回的具体原因
                reason = skip_reason or "未知原因"
                print(f"{progress_prefix} ID: {student_id} -> 已跳过 ({reason})")
                skipped_records.append(SkippedRecord(student_id=student_id, reason=reason))


    # 按 file_id 排序以保证输出顺序稳定
    all_student_forms.sort(key=lambda x: x.file_id)

    # 按 student_id 排序以保证输出顺序稳定
    skipped_records.sort(key=lambda x: x.student_id)

    # 写入文件
    writer = CsvWriter(OUTPUT_FILENAME)
    writer.write(all_student_forms)
    
    # 写入跳过记录文件
    skipped_writer = CsvWriter(SKIPPED_FILENAME)
    skipped_writer.write_skipped(skipped_records)


if __name__ == "__main__":
    asyncio.run(main())