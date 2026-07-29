"""Identity transform: passes every row through unchanged.

Used by the end-to-end tests in ``tests/e2e/test_pipeline_e2e.py`` as a
no-op transform, so a pipeline's extract and load stages can be asserted
on without a transform changing the data.
"""


def transform(data):
    """Pass through all rows unchanged."""
    return list(data)
