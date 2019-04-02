import requests
import pandas as pd
import io
from sqlalchemy import create_engine
import progressbar
import numpy as np
import json


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

    s_response = requests.get(url=smy_url).content

    smy_data = pd.read_csv(io.StringIO(s_response.decode('utf-8')))

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


def fetch_and_write_full_data(smy_schema_name, smy_table_name, agg_schema_name, agg_table_name, engine):
    append_or_replace_smy = "replace"
    append_or_replace_agg = "replace"
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

    a_response = requests.get(url=agg_url).content

    agg_data = pd.read_csv(io.StringIO(a_response.decode('utf-8')))

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


def main():
    conf = json.load(open("config.json"))
    password = conf["password"]
    engine = create_engine('postgresql://postgres:{}@localhost:5432/povcal'.format(password))
    test_smy_data = fetch_smy_data(poverty_line=1.9)
    test_agg_data = fetch_agg_data(poverty_line=1.9)
    existing_smy_data = fetch_old_smy_data("public", "PovCalNetSmy", '"PovertyLine" = 1.9', engine)
    existing_agg_data = fetch_old_agg_data("public", "PovCalNetAgg", '"povertyLine" = 1.9', engine)
    smy_is_same = smy_data_is_the_same(test_smy_data, existing_smy_data)
    agg_is_same = agg_data_is_the_same(test_agg_data, existing_agg_data)
    its_the_same = smy_is_same and agg_is_same
    if not its_the_same:
        fetch_and_write_full_data("public", "PovCalNetSmy", "public", "PovCalNetAgg", engine)
    else:
        print("No changes detected.")
    engine.dispose()


if __name__ == '__main__':
    main()
