import json
import sys
from pprint import pprint
from typing import Generator, Optional, Tuple

import pandas as pd


title_id_columns = ["Title", "Publisher", "Publisher_ID", "Platform", "Item_ID"]

flat_dimension_columns = [
    "YOP",
    "Access_Type",
    "Access_Method",
    "Data_Type",
    "Section_Type",
]


class C5Reader:

    base_columns = title_id_columns + flat_dimension_columns

    def __init__(self):
        pass

    @classmethod
    def create_df(cls, data):
        return pd.DataFrame(
            columns=[
                "Title",
                "Publisher",
                "Platform",
                "Item_ID",
                "YOP",
                "Access_Type",
                "Access_Method",
                "Data_Type",
                "Begin_Date",
                "End_Date",
                "Metric_Type",
                "Count",
            ],
        )

    def json_to_header_and_df(self, data: dict) -> Tuple[dict, pd.DataFrame]:
        records = []
        items = data.get("Report_Items", [])
        for item in items:
            for record in self.item_to_records(item):
                records.append(record)
        return data['Report_Header'], pd.DataFrame(records)

    def item_to_records(self, item: dict) -> Generator[dict, None, None]:
        base = {k: item.get(k, None) for k in self.base_columns}
        for rec in item["Performance"]:
            date_base = {
                "Begin_Date": rec["Period"]["Begin_Date"],
                "End_Date": rec["Period"]["End_Date"],
            }
            for instance in rec["Instance"]:
                yield {**instance, **date_base, **base}


class CounterFile:
    def __init__(
        self,
        fname: str,
        header: Optional[dict] = None,
        df: Optional[pd.DataFrame] = None,
    ):
        self.fname = fname
        self.header = header or {}
        self.df = df


def df_to_items(df: pd.DataFrame, expand_columns: [str]) -> [dict]:
    rollup_columns = [c for c in flat_dimension_columns if c not in expand_columns]
    group_by = ["Title", "Publisher", "Platform", *rollup_columns]
    items = []
    for key, grp in df.groupby(group_by):
        item = {k: grp.iloc[0][k] for k in title_id_columns + rollup_columns}
        perf = []
        for (start, end), recs in grp.groupby(["Begin_Date", "End_Date"]):
            perf.append(
                {
                    "Period": {
                        "Begin_Date": start,
                        "End_Date": end,
                    },
                    "Instance": [
                        {k: rec[k] for k in ["Metric_Type", "Count"] + expand_columns}
                        for _, rec in recs.iterrows()
                    ],
                }
            )
        item["Performance"] = perf
        items.append(item)
    return items


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help="json file to process")

    args = parser.parse_args()

    reader = C5Reader()
    with open(args.filename, "r") as infile:
        data = json.load(infile)
    orig_size = len(json.dumps(data))

    header, df = reader.json_to_header_and_df(data)
    #print(df.shape, file=sys.stderr)

    new_items = df_to_items(df, ["YOP", "Access_Type"])

    out = {'Report_Header': header, 'Report_Items': new_items}
    dump = json.dumps(out, ensure_ascii=False)
    print(dump)

    ratio = 100 * len(dump) / orig_size
    print(f'Unindented sizes: orig: {orig_size}, new: {len(dump)} ({ratio:.2f} %)', file=sys.stderr)

