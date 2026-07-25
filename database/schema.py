"""从 schema.sql 加载 DDL 字符串（延迟读取，避免 import 期文件缺失导致整个包不可导入）."""
import os

_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")


def _load_schema() -> str:
    try:
        with open(_SCHEMA_PATH) as f:
            return f.read()
    except FileNotFoundError:
        return ""


SCHEMA_SQL = _load_schema()
