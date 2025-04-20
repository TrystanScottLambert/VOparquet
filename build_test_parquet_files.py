"""
Build test cases.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree

# Create test DataFrame
df = pd.DataFrame(
    {
        "ra": [10.1, 20.2, 30.3, 40.4, 50.5],
        "dec": [-1.1, -2.2, -3.3, -4.4, -5.5],
        "redshift": [0.1, 0.2, 0.3, 0.4, 0.5],
    }
)


def create_votable_metadata():
    """
    Creates Metadata as a dataless XML VOTable.
    """
    NS = "http://www.ivoa.net/xml/VOTable/v1.3"
    NSMAP = {None: NS}

    votable = etree.Element("VOTABLE", version="1.4", nsmap=NSMAP)
    resource = etree.SubElement(votable, "RESOURCE")
    table = etree.SubElement(resource, "TABLE", name="TestTable")
    etree.SubElement(table, "DESCRIPTION").text = "Test table with RA, DEC, redshift"

    fields = [
        {
            "name": "ra",
            "datatype": "double",
            "ucd": "pos.eq.ra",
            "unit": "deg",
            "desc": "Right Ascension",
        },
        {
            "name": "dec",
            "datatype": "double",
            "ucd": "pos.eq.dec",
            "unit": "deg",
            "desc": "Declination",
        },
        {
            "name": "redshift",
            "datatype": "double",
            "ucd": "src.redshift",
            "unit": "",
            "desc": "Redshift",
        },
    ]

    for f in fields:
        field = etree.SubElement(table, "FIELD", name=f["name"], datatype=f["datatype"])
        if f["ucd"]:
            field.set("ucd", f["ucd"])
        if f["unit"]:
            field.set("unit", f["unit"])
        etree.SubElement(field, "DESCRIPTION").text = f["desc"]

    xml_bytes = etree.tostring(
        votable, pretty_print=True, encoding="UTF-8", xml_declaration=True
    )
    return xml_bytes.decode("utf-8")


if __name__ == '__main__':
    votable_metadata = create_votable_metadata()

    # Save plain Parquet file (just the table, no metadata)
    df.to_parquet("simple_table.parquet", index=False)

    # Create schema with metadata
    table = pa.Table.from_pandas(df)
    metadata = {
        b"IVOA.VOTable-Parquet.version": b"1.0",
        b"IVOA.VOTable-Parquet.content": votable_metadata.encode("utf-8"),
    }

    schema = table.schema.with_metadata(metadata)
    table_with_meta = pa.Table.from_arrays(table.columns, schema=schema)
    pq.write_table(table_with_meta, "voparquet_table.parquet")

    print("Created: simple_table.parquet and voparquet_table.parquet")
