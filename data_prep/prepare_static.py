import pandas as pd
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / "Data"
RAW = DATA / "raw"
OUT = DATA / "ved_static.csv"

ice = pd.read_excel(RAW / "VED_Static_Data_ICE&HEV.xlsx")
phev = pd.read_excel(RAW / "VED_Static_Data_PHEV&EV.xlsx")

ice = ice.rename(columns={"Vehicle Type": "EngineType"})
df = pd.concat([ice, phev], ignore_index=True)

df = df.rename(columns={
    "VehId": "veh_id",
    "EngineType": "engine_type",
    "Vehicle Class": "vehicle_class",
    "Engine Configuration & Displacement": "engine_config",
    "Transmission": "transmission",
    "Drive Wheels": "drive_wheels",
    "Generalized_Weight": "weight_lb",
})

df = df.replace("NO DATA", "")

df["engine_config"] = df["engine_config"].astype(str).str.strip()

assert df["veh_id"].is_unique, "veh_id not unique across the two files"

df.to_csv(OUT, index=False)
print(f"wrote {OUT} with {len(df)} rows")
