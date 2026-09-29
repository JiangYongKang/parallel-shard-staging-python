"""内容指纹计算与校验工具。

指纹统一规范为 ``algorithm:hex`` 字符串，例如 ``sha256:a1b2...``。
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable

# 出于安全与性能考虑，允许的指纹算法白名单
SUPPORTED_ALGORITHMS = frozenset({"sha256", "sha384", "sha512"})


def normalize_digest(value: str) -> str:
    """规范化指纹字符串为 ``algorithm:hex``。

    接受以下输入：
    - 裸 hex（默认按 sha256 解释）
    - ``algorithm:hex``
    - ``algorithm=hex``（兼容部分对象存储风格）

    非法格式抛 ``ValueError``。
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError("指纹不能为空")
    raw = value.strip().lower()
    if ":" in raw:
        algorithm, hex_digest = raw.split(":", 1)
    elif "=" in raw:
        algorithm, hex_digest = raw.split("=", 1)
    else:
        algorithm, hex_digest = "sha256", raw
    algorithm = algorithm.strip()
    hex_digest = hex_digest.strip()
    if algorithm not in SUPPORTED_ALGORITHMS:
        raise ValueError(f"不支持的指纹算法: {algorithm}")
    try:
        hashlib.new(algorithm, b"")
    except (ValueError, TypeError) as exc:  # pragma: no cover - 白名单内算法必然可用
        raise ValueError(f"不支持的指纹算法: {algorithm}") from exc
    expected_len = hashlib.new(algorithm).digest_size * 2
    if len(hex_digest) != expected_len:
        raise ValueError(
            f"{algorithm} 指纹长度应为 {expected_len} 个十六进制字符，实际 {len(hex_digest)}"
        )
    try:
        int(hex_digest, 16)
    except ValueError as exc:
        raise ValueError(f"指纹不是合法的十六进制串: {hex_digest!r}") from exc
    return f"{algorithm}:{hex_digest}"


def compute_digest(chunks: Iterable[bytes], algorithm: str = "sha256") -> str:
    """对字节块序列计算指纹，返回 ``algorithm:hex``。"""
    if algorithm not in SUPPORTED_ALGORITHMS:
        raise ValueError(f"不支持的指纹算法: {algorithm}")
    hasher = hashlib.new(algorithm)
    for chunk in chunks:
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise TypeError(f"字节块序列中出现非字节元素: {type(chunk).__name__}")
        hasher.update(chunk)
    return f"{algorithm}:{hasher.hexdigest()}"


def digest_to_blob_name(digest: str) -> str:
    """指纹 -> blob 文件名（仅取 hex 段）。"""
    return normalize_digest(digest).split(":", 1)[1]


def digest_algorithm(digest: str) -> str:
    """取指纹声明的算法名。"""
    return normalize_digest(digest).split(":", 1)[0]
