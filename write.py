"""
Writing helper functions
"""

from xml.etree.ElementTree import Element, SubElement, tostring
from typing import Optional

import pandas as pd


def generate_votable_xml(
    metadata_field_df: pd.DataFrame,
    description: Optional[str] = None,
    version: str = "1.3",
    param_df: Optional[pd.DataFrame] = None,
    info_df: Optional[pd.DataFrame] = None,
    coosys_df: Optional[pd.DataFrame] = None,
) -> str:
    """
    Builds the votable xml data structure from the given dataframes.
    """
    vot_ns = "http://www.ivoa.net/xml/VOTable/v1.3"
    votable = Element("VOTABLE", version=version, xmlns=vot_ns)
    resource = SubElement(votable, "RESOURCE")
    table = SubElement(resource, "TABLE")

    if description:
        SubElement(table, "DESCRIPTION").text = description

    if param_df is not None:
        for _, row in param_df.iterrows():
            param = SubElement(
                table, "PARAM", name=row["name"], datatype="char", arraysize="*"
            )
            param.set("value", str(row["value"]))

    if info_df is not None:
        for _, row in info_df.iterrows():
            info = SubElement(table, "INFO", name=row["name"], value=str(row["value"]))
            info.set("value", str(row["value"]))

    if coosys_df is not None:
        for _, row in coosys_df.iterrows():
            coosys = SubElement(table, "COOSYS")
            for attr, val in row.items():
                if pd.notna(val):
                    coosys.set(attr, str(val))

    for _, row in metadata_field_df.iterrows():
        field = SubElement(table, "FIELD")
        for attr in ["Name", "Unit", "UCD", "Datatype", "Arraysize"]:
            val = row.get(attr)
            if pd.notna(val):
                field.set(attr.lower() if attr != "Name" else "name", str(val))
        desc = row.get("Description")
        if pd.notna(desc):
            SubElement(field, "DESCRIPTION").text = str(desc)

    return tostring(votable, encoding="unicode")
