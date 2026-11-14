"""
CAMEO event taxonomy mappings.
Reference: https://data.gdeltproject.org/documentation/CAMEO.Manual.1.1b3.pdf
"""
from __future__ import annotations
from typing import Dict, Tuple

# Root code → (category name, is_conflict)
# QuadClass: 1=Verbal Cooperation, 2=Material Cooperation, 3=Verbal Conflict, 4=Material Conflict
CAMEO_ROOT_CATEGORIES: Dict[str, Tuple[str, bool]] = {
    "01": ("Make Public Statement", False),
    "02": ("Appeal", False),
    "03": ("Express Intent to Cooperate", False),
    "04": ("Consult", False),
    "05": ("Engage in Diplomatic Cooperation", False),
    "06": ("Engage in Material Cooperation", False),
    "07": ("Provide Aid", False),
    "08": ("Yield", False),
    "09": ("Investigate", False),
    "10": ("Demand", True),
    "11": ("Disapprove", True),
    "12": ("Reject", True),
    "13": ("Threaten", True),
    "14": ("Protest", True),
    "15": ("Exhibit Military Posture", True),
    "16": ("Reduce Relations", True),
    "17": ("Coerce", True),
    "18": ("Assault", True),
    "19": ("Fight", True),
    "20": ("Use Unconventional Mass Violence", True),
}

# QuadClass mapping
QUAD_CLASS_NAMES: Dict[int, str] = {
    1: "Verbal Cooperation",
    2: "Material Cooperation",
    3: "Verbal Conflict",
    4: "Material Conflict",
}

QUAD_CLASS_IS_CONFLICT: Dict[int, bool] = {
    1: False,
    2: False,
    3: True,
    4: True,
}

# GoldsteinScale interpretation thresholds
GOLDSTEIN_THRESHOLDS = [
    (7.0,  10.0,  "Strong Cooperation"),
    (3.0,  6.99,  "Moderate Cooperation"),
    (0.1,  2.99,  "Mild Cooperation"),
    (-0.1,  0.0,  "Neutral"),
    (-3.0, -0.11, "Mild Conflict"),
    (-7.0, -3.01, "Moderate Conflict"),
    (-10.0, -7.01,"Strong Conflict"),
]

# GDELT Theme prefix filters (remove noisy prefixes)
NOISY_THEME_PREFIXES = {
    "TAX_", "WB_", "USPEC_", "GENERAL_", "MEDIA_", "INTERNET_",
}

# Country FIPS code to ISO 3166-1 alpha-2 (sample — extend as needed)
FIPS_TO_ISO2: Dict[str, str] = {
    "US": "US", "UK": "GB", "FR": "FR", "GM": "DE", "RS": "RU",
    "CH": "CN", "JA": "JP", "IN": "IN", "BR": "BR", "MX": "MX",
    "AU": "AU", "CA": "CA", "IT": "IT", "SP": "ES", "NL": "NL",
    "SW": "SE", "NO": "NO", "DA": "DK", "FI": "FI", "PL": "PL",
    "EZ": "CZ", "HU": "HU", "RO": "RO", "BU": "BG", "GR": "GR",
    "TU": "TR", "IR": "IR", "IZ": "IQ", "SY": "SY", "SA": "SA",
    "EG": "EG", "LY": "LY", "MO": "MA", "AL": "DZ", "TN": "TN",
    "SF": "ZA", "NI": "NG", "GH": "GH", "ET": "ET", "KE": "KE",
    "ID": "ID", "TH": "TH", "VM": "VN", "PH": "PH", "MY": "MY",
    "SN": "SG", "PK": "PK", "BD": "BD", "AF": "AF",
    "UP": "UA", "PO": "PT", "BO": "BO", "CI": "CO", "AR": "AR",
    "VE": "VE", "PE": "PE", "CL": "CL", "IS": "IL", "JO": "JO",
    "LE": "LB", "MP": "NP", "MG": "MN", "KZ": "KZ", "UZ": "UZ",
}


def get_category_name(root_code: str) -> str:
    return CAMEO_ROOT_CATEGORIES.get(root_code, ("Unknown", False))[0]


def is_conflict(root_code: str) -> bool:
    return CAMEO_ROOT_CATEGORIES.get(root_code, ("Unknown", False))[1]


def goldstein_label(value: float) -> str:
    for lo, hi, label in GOLDSTEIN_THRESHOLDS:
        if lo <= value <= hi:
            return label
    return "Unknown"


def fips_to_iso2(fips: str) -> str:
    return FIPS_TO_ISO2.get(fips, fips)

