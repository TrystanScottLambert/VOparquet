"""
VOTable Class
"""

import io
from dataclasses import dataclass
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from astropy.io.votable.tree import VOTableFile, Resource, TableElement, Field, Info
from astropy.io.votable import parse

@dataclass
class VOParquetTable:
    """
    Main data structure for storing data and metadata of VOparquet files.
    """
    data: pd.DataFrame
    meta_data: VOTableFile
    version: str = "1.0"

    @classmethod
    def from_parquet(cls, filename: str) -> "VOParquetTable":
        """
        Creates a VOParquetTable object from the given file.
        """
        meta_data = read_vo_parquet_metadata(filename)
        data_frame = pq.read_table(filename).to_pandas()
        return cls(data_frame, meta_data)

    def write_to_parquet(self, out_file: str) -> None:
        """
        Writes the VoParquetTable Object to a parquet file.
        """
        table = pa.Table.from_pandas(self.data)
        metadata = dict(table.schema.metadata or {})

        # Write VOTable object to XML string
        buffer = io.BytesIO()
        self.meta_data.to_xml(buffer)
        votable_xml = buffer.getvalue().decode("utf-8")

        metadata[b"IVOA.VOTable-Parquet.content"] = votable_xml.encode("utf-8")
        metadata[b"IVOA.VOTable-Parquet.version"] = self.version.encode("utf-8")

        # Apply updated schema
        schema_with_meta = table.schema.with_metadata(metadata)
        table = table.cast(schema_with_meta)
        pq.write_table(table, out_file)

def read_vo_parquet_metadata(file_name: str) -> VOTableFile:
    """
    Reading the parquet meta data according to VOParquet standards.
    """
    meta = pq.read_metadata(file_name)
    xml_bytes = meta.metadata[b"IVOA.VOTable-Parquet.content"]
    buffer = io.BytesIO(xml_bytes)
    return parse(buffer)


if __name__ == '__main__':
    # Create a new VOTable file...
    votable = VOTableFile()
    resource = Resource()
    votable.resources.append(resource)
    table = TableElement(votable)
    resource.tables.append(table)
    table.fields.extend([
            Field(votable, name="ra", datatype="double", arraysize="*", unit="deg"),
            Field(votable, name="dec", datatype="double", arraysize="*", unit="deg"),
            Field(votable, name="z", datatype="double", arraysize="*", unit="")])
    table.description = "This is a table"
    table.infos.extend([
        Info("SHARK-VERSION", "SHARKv2", "v1.2")
    ])
    data = pd.DataFrame({"ra": [10.1, 20.2], "dec": [-30.1, -45.3], "z": [0.03, 0.1]})
    vp = VOParquetTable(data, votable)
    vp.write_to_parquet('test_again.parquet')
