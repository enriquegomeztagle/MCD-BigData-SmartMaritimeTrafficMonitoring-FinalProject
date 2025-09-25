import streamlit as st


def get_table_name():
    return st.secrets.get("BQ_TABLE", "river-treat-468823.ais_data.ais_messages")


def build_date_filter(start_date, end_date, date_column="BaseDateTime"):
    return f"AND DATE({date_column}) >= '{start_date}' AND DATE({date_column}) <= '{end_date}'"


def build_vessel_filter(vessel_types, column="VesselTypeName"):
    if not vessel_types:
        return ""
    vessel_list = "', '".join(vessel_types)
    return f"AND {column} IN ('{vessel_list}')"


def build_mmsi_filter(mmsi_list):
    if not mmsi_list:
        return ""
    mmsi_str = "', '".join(map(str, mmsi_list))
    return f"AND MMSI IN ('{mmsi_str}')"


def build_bbox_filter(lat_min, lat_max, lon_min, lon_max, use_bbox=True):
    if not use_bbox:
        return ""
    return f"AND LAT BETWEEN {lat_min} AND {lat_max} AND LON BETWEEN {lon_min} AND {lon_max}"
