from dataclasses import dataclass, field as dc_field
from enum import Enum
from typing import List, Optional
from astropy.io.votable.tree import VOTableFile, Resource, TableElement, Field, Param, Info, Coosys, Timesys

import pandas as pd

class CoosysSystem(str, Enum):
    EQ_FK5 = "eq_FK5"
    EQ_FK4 = "eq_FK4"
    GALACTIC = "galactic"
    ICRS = "ICRS"

class TimesysScale(str, Enum):
    TT = "TT"
    UTC = "UTC"
    TAI = "TAI"
    GPS = "GPS"
    UT1 = "UT1"
    TCG = "TCG"
    TCB = "TCB"
    TDB = "TDB"
    LST = "LST"

class RefPosition(str, Enum):
    TOPOCENTER = "TOPOCENTER"
    GEOCENTER = "GEOCENTER"
    BARYCENTER = "BARYCENTER"
    HELIOCENTER = "HELIOCENTER"
    LSR = "LSR"

@dataclass
class CoosysMeta:
    ID: str
    system: CoosysSystem
    equinox: Optional[str] = None
    epoch: Optional[str] = None

@dataclass
class TimesysMeta:
    ID: str
    timescale: TimesysScale
    refposition: RefPosition
    timeorigin: Optional[str] = None

@dataclass
class ParquetMetaVO:
    fields: pd.DataFrame
    params: Optional[List[dict]] = None
    infos: Optional[List[dict]] = None
    coosys: Optional[List[CoosysMeta]] = None
    timesys: Optional[List[TimesysMeta]] = None
    description: Optional[str] = None

    def to_votable(self) -> VOTableFile:
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
                arraysize=getattr(field_row, "ArraySize", None)
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
                    datatype=param_dict.get("datatype", "char")
                )
                for attr, val in param_dict.items():
                    if attr not in {"name", "value", "datatype"} and val is not None:
                        setattr(param, attr, val)
                table_element.params.append(param)

        if self.infos:
            for info_dict in self.infos:
                if not all(k in info_dict for k in ("name", "value")):
                    raise ValueError("INFO must include at least 'name' and 'value'")
                info = Info(
                    name=info_dict["name"],
                    value=info_dict["value"]
                )
                for attr, val in info_dict.items():
                    if attr not in {"name", "value"} and val is not None:
                        setattr(info, attr, val)
                table_element.infos.append(info)

        if self.coosys:
            if not hasattr(resource, "_coosystems"):
                resource._coosystems = []
            for coosys_entry in self.coosys:
                coosys = Coosys(
                    ID=coosys_entry.ID,
                    system=coosys_entry.system,
                    equinox=coosys_entry.equinox,
                    epoch=coosys_entry.epoch
                )
                resource._coosystems.append(coosys)

        if self.timesys:
            if not hasattr(resource, "_timesystems"):
                resource._timesystems = []
            for timesys_entry in self.timesys:
                timesys = Timesys(
                    ID=timesys_entry.ID,
                    timescale=timesys_entry.timescale,
                    refposition=timesys_entry.refposition,
                    timeorigin=timesys_entry.timeorigin
                )
                resource._timesystems.append(timesys)

        return votable

    @classmethod
    def from_votable(cls, votable: VOTableFile) -> "ParquetMetaVO":
        resource = votable.resources[0]
        table = resource.tables[0]

        # Fields
        field_data = []
        for field_element in table.fields:
            field_data.append({
                "Name": field_element.name,
                "Datatype": field_element.datatype,
                "UCD": field_element.ucd,
                "Unit": field_element.unit,
                "ArraySize": field_element.arraysize,
                "Description": getattr(field_element.description, 'value', None) if field_element.description else None
            })
        fields_df = pd.DataFrame(field_data)

        # Params
        param_data = []
        for param in table.params:
            param_dict = {"name": param.name, "value": param.value, "datatype": param.datatype}
            if param.unit is not None:
                param_dict["unit"] = param.unit
            param_data.append(param_dict)

        # Infos
        info_data = []
        for info in table.infos:
            info_data.append({
                "name": info.name,
                "value": info.value,
                "ID": getattr(info, "ID", None),
                "Content": getattr(info, "content", None)
            })

        # COOSYS
        coosys_list = []
        if hasattr(resource, "_coosystems"):
            for cs in resource._coosystems:
                coosys_list.append(CoosysMeta(
                    ID=cs.ID,
                    system=CoosysSystem(cs.system),
                    equinox=cs.equinox,
                    epoch=cs.epoch
                ))

        # TIMESYS
        timesys_list = []
        if hasattr(resource, "_timesystems"):
            for ts in resource._timesystems:
                timesys_list.append(TimesysMeta(
                    ID=ts.ID,
                    timescale=TimesysScale(ts.timescale),
                    refposition=RefPosition(ts.refposition),
                    timeorigin=ts.timeorigin
                ))

        return cls(
            fields=fields_df,
            params=param_data or None,
            infos=info_data or None,
            coosys=coosys_list or None,
            timesys=timesys_list or None,
            description=table.description.value if table.description else None
        )
