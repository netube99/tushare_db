"""Bin 文件读写 — write_bin / append_bin / 校验."""

import os
from pathlib import Path

import numpy as np


def write_bin(path: Path, values: np.ndarray) -> bool:
    """写入单个 .day.bin 文件（原子写：tmp + os.replace）.

    Args:
        path: 输出文件路径
        values: 长度为日历天数的 float32 数组（NaN 表示无数据）

    Returns:
        True 如果写入了数据，False 如果全为 NaN（不创建文件）
    """
    valid_mask = ~np.isnan(values)
    if not valid_mask.any():
        return False

    first_valid = int(valid_mask.argmax())
    last_valid = int(len(values) - valid_mask[::-1].argmax() - 1)
    trimmed = values[first_valid:last_valid + 1]

    data = np.hstack([
        np.array([first_valid], dtype=np.float32),
        trimmed.astype(np.float32),
    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    data.astype("<f").tofile(str(tmp_path))
    os.replace(str(tmp_path), str(path))
    return True


def append_bin(path: Path, new_values: np.ndarray) -> bool:
    """向现有 bin 文件追加新数据.

    Args:
        path: 已有 bin 文件路径
        new_values: 完整日历长度的 float32 数组
    """
    if not path.exists():
        return write_bin(path, new_values)

    try:
        with open(path, "rb") as f:
            header = np.frombuffer(f.read(4), dtype="<f")
            if len(header) == 0:
                raise ValueError("文件头损坏: 空文件")
            start_idx = int(header[0])
            f.seek(0, 2)
            file_size = f.tell()
            if file_size < 4 or (file_size - 4) % 4 != 0:
                raise ValueError(f"文件损坏: size={file_size}")
            existing_len = (file_size // 4) - 1
    except (ValueError, OSError, IndexError) as e:
        import logging
        logging.getLogger("convert_to_qlib").warning(
            f"append_bin: {path} 文件损坏 ({e})，将全量重建"
        )
        path.unlink(missing_ok=True)
        return write_bin(path, new_values)

    expected_end = start_idx + existing_len
    if expected_end >= len(new_values):
        return False

    tail = new_values[expected_end:]
    valid_mask = ~np.isnan(tail)
    if not valid_mask.any():
        return False
    last_valid = len(tail) - valid_mask[::-1].argmax() - 1
    tail_trimmed = tail[:last_valid + 1]
    with open(path, "ab") as f:
        tail_trimmed.astype("<f").tofile(f)
    return True
