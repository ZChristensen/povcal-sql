import requests
import pandas as pd
import io
from sqlalchemy import create_engine
import progressbar
import numpy as np
import json


def fetch_data(poverty_line):
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


def fetch_old_data(schema_name, table_name, boundary, engine):
    try:
        return pd.read_sql_query('SELECT * FROM "{}"."{}" WHERE {};'.format(schema_name, table_name, boundary), engine)
    except:
        return pd.DataFrame(columns=["CountryCode", "RequestYear", "PovertyLine", "HeadCount"])


def data_is_the_same(new_data, old_data):
    id_vars = ["CountryCode", "RequestYear", "PovertyLine"]
    val_vars = id_vars + ["HeadCount"]
    n_sorted = new_data.sort_values(by=id_vars).round({'HeadCount': 3})[val_vars]
    o_sorted = old_data.sort_values(by=id_vars).round({'HeadCount': 3})[val_vars]
    return n_sorted.equals(o_sorted)


def fetch_and_write_full_data(schema_name, table_name, engine):
    append_or_replace = "replace"
    for povline in progressbar.progressbar(np.linspace(0.01, 10, 1000)):
        pov_data = fetch_data(poverty_line=povline)
        pov_data.to_sql(name="PovCalNetSmy", con=engine, schema="public", index=False, if_exists=append_or_replace)
        append_or_replace = "append"


def main():
    conf = json.load(open("config.json"))
    password = conf["password"]
    engine = create_engine('postgresql://postgres:{}@localhost:5432/povcal'.format(password))
    test_data = fetch_data(poverty_line=1.9)
    existing_data = fetch_old_data("public", "PovCalNetSmy", '"PovertyLine" = 1.9', engine)
    its_the_same = data_is_the_same(test_data, existing_data)
    if not its_the_same:
        fetch_and_write_full_data("public", "PovCalNetSmy", engine)
    else:
        print("No changes detected.")
    engine.dispose()


if __name__ == '__main__':
    main()
