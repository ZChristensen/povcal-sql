import requests
import pandas as pd
import io
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import progressbar
import numpy as np
import json
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import date


def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def seq(from_num, to_num, by_step, endpoint=False):
    num_steps = round((to_num-from_num)/by_step)
    return np.linspace(from_num, to_num, num_steps, endpoint=endpoint)


def fetch_smy_data(poverty_line):
    """Fetch data from PovCalNet at poverty_line."""
    url = "http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx?"
    smy_params = {
        "Countries": "all",
        "GroupedBy": "WB",
        "PovertyLine": str(poverty_line),
        "RefYears": "all",
        "Display": "C",
        "format": "csv"
    }

    smy_url = url + "&".join(["{}={}".format(item[0], item[1]) for item in smy_params.items()])

    s_response = requests_retry_session().get(url=smy_url, timeout=30).content

    smy_data = pd.read_csv(io.StringIO(s_response.decode('utf-8',errors='ignore')))

    return smy_data


def fetch_old_smy_data(schema_name, table_name, boundary, engine):
    try:
        return pd.read_sql_query('SELECT * FROM "{}"."{}" WHERE {};'.format(schema_name, table_name, boundary), engine)
    except:
        return pd.DataFrame(columns=["CountryCode", "RequestYear", "PovertyLine", "HeadCount"])


def smy_data_is_the_same(new_data, old_data):
    id_vars = ["CountryCode", "RequestYear", "PovertyLine"]
    val_vars = id_vars + ["HeadCount"]
    n_sorted = new_data.sort_values(by=id_vars).round({'HeadCount': 3})[val_vars]
    o_sorted = old_data.sort_values(by=id_vars).round({'HeadCount': 3})[val_vars]
    return n_sorted.equals(o_sorted)

def fetch_svy_data(poverty_line):
    """Fetch data from PovCalNet at poverty_line."""
    url = "http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx?"
    smy_params_svy = {
        "Countries": "all",
        "GroupedBy": "WB",
        "PovertyLine": str(poverty_line),
        "SurveyYears": "all",
        "Display": "C",
        "format": "csv"
    }

    smy_url_svy = url + "&".join(["{}={}".format(item[0], item[1]) for item in smy_params_svy.items()])

    s_response = requests_retry_session().get(url=smy_url_svy, timeout=30).content

    smy_data_svy = pd.read_csv(io.StringIO(s_response.decode('utf-8',errors='ignore')))

    return smy_data_svy


def fetch_old_svy_data(schema_name, table_name, boundary, engine):
    try:
        return pd.read_sql_query('SELECT * FROM "{}"."{}" WHERE {};'.format(schema_name, table_name, boundary), engine)
    except:
        return pd.DataFrame(columns=["CountryCode", "DataYear", "PovertyLine", "HeadCount"])


def svy_data_is_the_same(new_data, old_data):
    id_vars = ["CountryCode", "DataYear", "PovertyLine"]
    val_vars = id_vars + ["HeadCount"]
    n_sorted = new_data.sort_values(by=id_vars).round({'HeadCount': 3})[val_vars]
    o_sorted = old_data.sort_values(by=id_vars).round({'HeadCount': 3})[val_vars]
    return n_sorted.equals(o_sorted)


def fetch_and_write_full_data(smy_schema_name, smy_table_name, agg_schema_name, agg_table_name, svy_schema_name, svy_table_name, engine):
    append_or_replace_smy = "replace"
    append_or_replace_agg = "replace"
    append_or_replace_svy = "replace"
    total_sequence = np.concatenate(
        (
            seq(0, 10, 0.01),
            seq(10, 25.5, 0.25),
            seq(25.5, 36, 0.025),
            seq(36, 500, 1),
            seq(500, 805, 5)
        )
    )
    for povline in progressbar.progressbar(total_sequence):
        smy_pov_data = fetch_smy_data(poverty_line=povline)
        smy_pov_data.to_sql(name="PovCalNetSmy", con=engine, schema="public", index=False, if_exists=append_or_replace_smy)
        append_or_replace_smy = "append"
        agg_pov_data = fetch_agg_data(poverty_line=povline)
        agg_pov_data.to_sql(name="PovCalNetAgg", con=engine, schema="public", index=False, if_exists=append_or_replace_agg)
        append_or_replace_agg = "append"
        svy_pov_data = fetch_svy_data(poverty_line=povline)
        svy_pov_data.to_sql(name="PovCalNetSvy", con=engine, schema="public", index=False, if_exists=append_or_replace_svy)
        append_or_replace_svy = "append"



def fetch_agg_data(poverty_line):
    """Fetch data from PovCalNet at poverty_line."""
    url = "http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx?"
    agg_params = {
        "Countries": "all",
        "GroupedBy": "WB",
        "povertyLine": str(poverty_line),
        "RefYears": "all",
        "Display": "Regional",
        "format": "csv"
    }

    agg_url = url + "&".join(["{}={}".format(item[0], item[1]) for item in agg_params.items()])

    a_response = requests_retry_session().get(url=agg_url, timeout=30).content

    agg_data = pd.read_csv(io.StringIO(a_response.decode('utf-8', errors='ignore')))

    return agg_data


def fetch_old_agg_data(schema_name, table_name, boundary, engine):
    try:
        return pd.read_sql_query('SELECT * FROM "{}"."{}" WHERE {};'.format(schema_name, table_name, boundary), engine)
    except:
        return pd.DataFrame(columns=["regionCID", "requestYear", "povertyLine", "hc"])


def agg_data_is_the_same(new_data, old_data):
    id_vars = ["regionCID", "requestYear", "povertyLine"]
    val_vars = id_vars + ["hc"]
    n_sorted = new_data.sort_values(by=id_vars).round({'hc': 3})[val_vars]
    o_sorted = old_data.sort_values(by=id_vars).round({'hc': 3})[val_vars]
    return n_sorted.equals(o_sorted)



def save_backup(smy_schema_name, smy_table_name, agg_schema_name, agg_table_name, svy_schema_name, svy_table_name, engine):
    today_str = str(date.today()).replace("-","")
    with engine.connect() as con:
        try:
            smy_copy_command = text('create table "{}"."{}" as table "{}"."{}"'.format(smy_schema_name,smy_table_name+today_str,smy_schema_name,smy_table_name))
            con.execute(smy_copy_command)
        except:
            pass
        try:
            agg_copy_command = text('create table "{}"."{}" as table "{}"."{}"'.format(agg_schema_name,agg_table_name+today_str,agg_schema_name,agg_table_name))
            con.execute(agg_copy_command)
        except:
            pass
        try:
            svy_copy_command = text('create table "{}"."{}" as table "{}"."{}"'.format(svy_schema_name,svy_table_name+today_str,svy_schema_name,svy_table_name))
            con.execute(svy_copy_command)
        except:
            pass


def main():
    conf = json.load(open("config.json"))
    password = conf["password"]
    engine = create_engine('postgresql://postgres:{}@localhost:5432/povcal'.format(password))
    test_smy_data = fetch_smy_data(poverty_line=1.9)
    test_svy_data = fetch_svy_data(poverty_line=1.9)
    test_agg_data = fetch_agg_data(poverty_line=1.9)
    existing_smy_data = fetch_old_smy_data("public", "PovCalNetSmy", '"PovertyLine" = 1.9', engine)
    existing_agg_data = fetch_old_agg_data("public", "PovCalNetAgg", '"povertyLine" = 1.9', engine)
    existing_svy_data = fetch_old_svy_data("public", "PovCalNetSvy", '"povertyLine" = 1.9', engine)
    smy_is_same = smy_data_is_the_same(test_smy_data, existing_smy_data)
    svy_is_same = svy_data_is_the_same(test_svy_data, existing_svy_data)
    agg_is_same = agg_data_is_the_same(test_agg_data, existing_agg_data)
    its_the_same = smy_is_same and agg_is_same and svy_is_same
    if not its_the_same:
        save_backup("public", "PovCalNetSmy", "public", "PovCalNetAgg","public", "PovCalNetSvy", engine)
        fetch_and_write_full_data("public", "PovCalNetSmy", "public", "PovCalNetAgg","public", "PovCalNetSvy", engine)
    else:
        print("No changes detected.")
    engine.dispose()


if __name__ == '__main__':
    main()
