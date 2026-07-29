"""Custom transform: derive a sales region from the country code.

Used by the custom+AI combination examples to prove that custom code and
AI-generated code compose in the configured order without clobbering each
other's columns.
"""

_REGIONS = {"US": "AMER", "GB": "EMEA", "NL": "EMEA"}


def transform(data):
    out = []
    for row in data:
        new = dict(row)
        new["region"] = _REGIONS.get(row.get("country"), "OTHER")
        out.append(new)
    return out
