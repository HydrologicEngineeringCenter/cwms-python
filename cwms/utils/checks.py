import base64


def is_base64(s: str) -> bool:
    """Check if a string is Base64 encoded."""
    try:
        decoded = base64.b64decode(s, validate=True)
        return base64.b64encode(decoded).decode("utf-8") == s
    except (ValueError, TypeError):
        return False
