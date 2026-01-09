import base64


def is_base64(s: str) -> bool:
    """Check if a string is Base64 encoded."""
    try:
        decoded = base64.b64decode(s, validate=True)
        return base64.b64encode(decoded).decode("utf-8") == s
    except (ValueError, TypeError):
        return False


def has_invalid_chars(id: str) -> bool:
    """
    Checks if ID contains any invalid web path characters.
    """
    INVALID_PATH_CHARS = ["/", "\\", "&", "?", "="]

    for char in INVALID_PATH_CHARS:
        if char in id:
            return True
    return False
