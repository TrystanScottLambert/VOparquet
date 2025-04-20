"""
Reading helper functions
"""

import xml.etree.ElementTree as ET
from typing import Optional, Tuple
import pandas as pd


def extract_votable_metadata(
    xml_string: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, Optional[str]]:
    """
    Creates the appropriate dataframes from the given xml string.
    """
    root = ET.fromstring(xml_string)
    ns = {"v": root.tag.split("}")[0].strip("{")}

    desc_el = root.find(".//v:TABLE/v:DESCRIPTION", ns)
    description = desc_el.text.strip() if desc_el is not None else None

    def extract_elements(tag: str, attribs: list) -> pd.DataFrame:
        elements = root.findall(f".//v:{tag}", ns)
        rows = []
        for el in elements:
            rows.append({attr: el.attrib.get(attr) for attr in attribs})
        return pd.DataFrame(rows) if rows else pd.DataFrame(columns=attribs)

    field_df = extract_elements(
        "FIELD", ["name", "unit", "ucd", "datatype", "arraysize"]
    )
    field_df.rename(
        columns={
            "name": "Name",
            "unit": "Unit",
            "ucd": "UCD",
            "datatype": "Datatype",
            "arraysize": "Arraysize",
        },
        inplace=True,
    )
    field_df["Description"] = [
        el.findtext("v:DESCRIPTION", default="", namespaces=ns)
        for el in root.findall(".//v:FIELD", ns)
    ]

    param_df = extract_elements("PARAM", ["name", "value"])
    info_df = extract_elements("INFO", ["name", "value"])
    coosys_df = extract_elements("COOSYS", ["ID", "equinox", "epoch", "system"])

    return field_df, param_df, info_df, coosys_df, description
