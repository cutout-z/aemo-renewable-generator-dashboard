"""Synthetic stand-ins for AEMO files, built in memory for offline tests."""

from __future__ import annotations

import io
import zipfile

import pandas as pd

REG_COLUMNS = [
    "Participant", "Station Name", "Region", "Dispatch Type", "Category",
    "Classification", "Fuel Source - Primary", "Fuel Source - Descriptor",
    "Technology Type - Primary", "Technology Type - Descriptor", "Units",
    "Aggregation", "DUID", "Reg Cap generation (MW)",
]


def reg_row(duid, station, region, fuel, tech, mw, primary_tech="Renewable"):
    return {
        "Participant": "Test Pty Ltd", "Station Name": station, "Region": region,
        "Dispatch Type": "Generator", "Category": "Market",
        "Classification": "Semi-Scheduled", "Fuel Source - Primary": fuel,
        "Fuel Source - Descriptor": fuel, "Technology Type - Primary": primary_tech,
        "Technology Type - Descriptor": tech, "Units": 1, "Aggregation": "Y",
        "DUID": duid, "Reg Cap generation (MW)": mw,
    }


DEFAULT_REG_ROWS = [
    reg_row("WANDSF1", "Wandoan Solar Farm", "QLD1", "Solar", "Photovoltaic Tracking Flat panel", 159),
    reg_row("WANDSF2", "Wandoan Solar Farm", "QLD1", "Solar", "Photovoltaic Tracking Flat panel", 310),
    reg_row("ARWF1", "Ararat Wind Farm", "VIC1", "Wind", "Wind - Onshore", 240),
    reg_row("ER01", "Eraring", "NSW1", "Fossil", "Steam Sub-Critical", 720, primary_tech="Combustion"),
]


def registration_xlsx(rows=None, sheet="PU and Scheduled Loads") -> bytes:
    df = pd.DataFrame(rows if rows is not None else DEFAULT_REG_ROWS, columns=REG_COLUMNS)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        pd.DataFrame({"x": [1]}).to_excel(xw, sheet_name="Read Me", index=False)
        df.to_excel(xw, sheet_name=sheet, index=False)
    return buf.getvalue()


CHALLENGE_PAGE = (b'<!DOCTYPE html><html lang="en-US"><head><title>Just a moment...</title>'
                  b"</head><body></body></html>")

DUD_COLUMNS = ["DUID", "START_DATE", "END_DATE", "DISPATCHTYPE", "CONNECTIONPOINTID",
               "REGIONID", "STATIONID", "PARTICIPANTID", "LASTCHANGED"]


def dudetail_zip(rows) -> bytes:
    """rows: (DUID, START_DATE 'YYYY/MM/DD', DISPATCHTYPE, REGIONID, STATIONID)."""
    lines = [
        "C,SETP.WORLD,DVD_DUDETAILSUMMARY,AEMO,PUBLIC,2026/09/25,13:40:25,1,MONTHLY_ARCHIVE,1",
        "I,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,7," + ",".join(DUD_COLUMNS),
    ]
    for duid, start, dtype, region, station in rows:
        lines.append(
            f'D,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,7,{duid},"{start} 00:00:00",'
            f'"2999/12/31 00:00:00",{dtype},XX1,{region},{station},PART,"2026/09/24 11:50:44"'
        )
    lines.append("C,END OF REPORT,3")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("PUBLIC_ARCHIVE#DUDETAILSUMMARY#FILE01#202608010000.CSV", "\n".join(lines))
    return buf.getvalue()


class FakeResponse:
    def __init__(self, status_code=200, content=b"", headers=None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}
        self.text = content.decode("utf-8", errors="replace")

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"HTTP {self.status_code}")


GEN_INFO_COLUMNS = [" Survey ID", "Site Name", "Site Owner", "Region", "Unit Name",
                    "Technology Type", "Technology Detail", "DUID", "Dispatch Type",
                    "Aggregated Nameplate Capacity (MW AC)", "Commitment Status"]


def gen_info_xlsx(rows=None, extra_columns=None) -> bytes:
    """A Generation Information workbook shaped like the July 2026 edition."""
    rows = rows if rows is not None else [
        [2001, "Wandoan South Solar Stage 2", "Owner", "QLD1", "U1", "Solar PV",
         "Single Axis Tracking", "WANDSF2", "Semi-Scheduled", 310, "In Service"],
        [2002, "Ararat Wind Farm", "Owner", "VIC1", "U1", "Wind", "Onshore",
         "ARWF1", "Semi-Scheduled", 240, "In Service"],
        [2003, "Kidston Pumped Hydro", "Owner", "QLD1", "U2", "Hydro", "Pumped",
         "KIDSPHG2", "Scheduled", 125, "In Commissioning"],
    ]
    cols = GEN_INFO_COLUMNS + list(extra_columns or [])
    body = pd.DataFrame(rows, columns=cols)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        pd.DataFrame({"Disclaimer": ["..."]}).to_excel(xw, sheet_name="Disclaimer", index=False)
        preamble = pd.DataFrame([["Existing Generators & New Developments"], ["note"], ["note"]])
        preamble.to_excel(xw, sheet_name="Generator Information", index=False, header=False)
        body.to_excel(xw, sheet_name="Generator Information", index=False, startrow=3)
    return buf.getvalue()
