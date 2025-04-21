"""
Data object for the Parquet File Meta data as a VO table.
"""

from dataclasses import dataclass
from typing import List, Optional
from astropy.io.votable.tree import (
    VOTableFile,
    Resource,
    TableElement,
    Field,
    Param,
    Info,
)
import pandas as pd
import numpy as np

def dtype_to_vodatatype(dtype: np.dtype) -> str:
    """
    Map a pandas/numpy dtype to a VOTable datatype string.
    Raises ValueError if dtype is unsupported.
    """
    if pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    elif pd.api.types.is_integer_dtype(dtype):
        if dtype == np.uint8:
            return "unsignedByte"
        elif dtype == np.int16:
            return "short"
        elif dtype == np.int32:
            return "int"
        elif dtype == np.int64:
            return "long"
    elif pd.api.types.is_float_dtype(dtype):
        if dtype == np.float32:
            return "float"
        elif dtype == np.float64:
            return "double"
    elif pd.api.types.is_complex_dtype(dtype):
        if dtype == np.complex64:
            return "floatComplex"
        elif dtype == np.complex128:
            return "doubleComplex"
    elif pd.api.types.is_string_dtype(dtype):
        return "unicodeChar"
    elif pd.api.types.is_object_dtype(dtype):
        # Fallback for strings often stored as object
        return "unicodeChar"

    raise ValueError(f"Unsupported dtype for VOTable: {dtype}")

def get_names_and_datatypes(data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Tries to build the field Dataframe from the actual data.
    """
    names = list(data_df.columns)
    data_type = [dtype_to_vodatatype(data_df[name].dtype) for name in names]
    return pd.DataFrame.from_dict({"Name": names, "Datatype": data_type})


@dataclass
class ParquetMetaVO:
    """
    Main Class for storing Parquet meta data as a VO Table.
    """

    fields: pd.DataFrame
    params: Optional[List[dict]] = None
    infos: Optional[List[dict]] = None
    description: Optional[str] = None

    def to_votable(self) -> VOTableFile:
        """
        Converts the ParquetMetaVO to a standard astropy VOTableFile
        """
        votable = VOTableFile()
        resource = Resource()
        votable.resources.append(resource)
        table_element = TableElement(votable)
        resource.tables.append(table_element)

        if self.description:
            table_element.description = self.description

        for field_row in self.fields.itertuples(index=False):
            field = Field(
                votable,
                name=getattr(field_row, "Name", None),
                datatype=getattr(field_row, "Datatype", None),
                unit=getattr(field_row, "Unit", None),
                ucd=getattr(field_row, "UCD", None),
                arraysize=getattr(field_row, "ArraySize", None),
            )
            description = getattr(field_row, "Description", None)
            if pd.notna(description):
                field.description = description
            table_element.fields.append(field)

        if self.params:
            for param_dict in self.params:
                if not all(k in param_dict for k in ("name", "value")):
                    raise ValueError("PARAM must include at least 'name' and 'value'")
                param = Param(
                    votable,
                    name=param_dict["name"],
                    value=param_dict["value"],
                    datatype=param_dict.get("datatype", "char"),
                )
                for attr, val in param_dict.items():
                    if attr not in {"name", "value", "datatype"} and val is not None:
                        setattr(param, attr, val)
                table_element.params.append(param)

        if self.infos:
            for info_dict in self.infos:
                if not all(k in info_dict for k in ("name", "value")):
                    raise ValueError("INFO must include at least 'name' and 'value'")
                info = Info(name=info_dict["name"], value=info_dict["value"])
                for attr, val in info_dict.items():
                    if attr not in {"name", "value"} and val is not None:
                        setattr(info, attr, val)
                table_element.infos.append(info)

        return votable

    @classmethod
    def from_votable(cls, votable: VOTableFile) -> "ParquetMetaVO":
        """
        Creates a ParquetMetaVO from a valid VOTableFile.
        """
        resource = votable.resources[0]
        table = resource.tables[0]

        # Fields
        field_data = []
        for field_element in table.fields:
            field_data.append(
                {
                    "Name": field_element.name,
                    "Datatype": field_element.datatype,
                    "UCD": field_element.ucd,
                    "Unit": field_element.unit,
                    "ArraySize": field_element.arraysize,
                    "Description": (
                        getattr(field_element.description, "value", None)
                        if field_element.description
                        else None
                    ),
                }
            )
        fields_df = pd.DataFrame(field_data)

        # Params
        param_data = []
        for param in table.params:
            param_dict = {
                "name": param.name,
                "value": param.value,
                "datatype": param.datatype,
            }
            if param.unit is not None:
                param_dict["unit"] = param.unit
            param_data.append(param_dict)

        # Infos
        info_data = []
        for info in table.infos:
            info_data.append(
                {
                    "name": info.name,
                    "value": info.value,
                    "ID": getattr(info, "ID", None),
                    "Content": getattr(info, "content", None),
                }
            )

        return cls(
            fields=fields_df,
            params=param_data or None,
            infos=info_data or None,
            description=table.description if table.description else None,
        )


if __name__ == "__main__":
    fields = pd.DataFrame.from_dict(
        {
            "Name": ["RA", "DEC", "Z"],
            "Unit": ["deg", "deg", ""],
            "Datatype": ["double", "double", "double"],
            "Description": ["Right Ascension", "Declination", "Redshift"],
        }
    )
    infos = [{"Name": "SHARKv2-VERSION", "Value": "2.1"}, {"Name": "Author", "Value": "Trytan"}]
    params = [{"Name": "OmegaM", "Value": 0.3}]
    vp = ParquetMetaVO(fields, params, infos, "This is a test table by me.")
