def transform(data):
    """Pass through all rows unchanged.

    Used by the smoke harness so that output row count == input row count,
    making "exit 0 but empty output" (the BUG-2 class) detectable.
    """
    return list(data)
