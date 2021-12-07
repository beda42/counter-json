import json
import sys
from concurrent.futures import ProcessPoolExecutor, wait
from pprint import pprint
from typing import Generator, Optional, Tuple

import pandas as pd

from utils import total_size

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

    def json_to_header_and_df(self, data: dict) -> Tuple[dict, pd.DataFrame]:
        records = []
        items = data.get("Report_Items", [])
        for item in items:
            for record in self.item_to_records(item):
                records.append(record)
        return data["Report_Header"], pd.DataFrame(records)

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


def df_to_items(
    df: pd.DataFrame, expand_columns: [str], date_merge: bool = False
) -> [dict]:
    rollup_columns = [c for c in flat_dimension_columns if c not in expand_columns]
    group_by = ["Title", "Publisher", "Platform", *rollup_columns]
    items = []
    for key, grp in df.groupby(group_by, dropna=False):
        item = {k: grp.iloc[0][k] for k in title_id_columns + rollup_columns}
        perf = []
        if not date_merge:
            for (start, end), recs in grp.groupby(["Begin_Date", "End_Date"], dropna=False):
                perf_groups = []
                perf_rec = {
                    "Period": {
                        "Begin_Date": start,
                        "End_Date": end,
                    },
                    "Groups": perf_groups,
                }
                perf.append(perf_rec)
                for subgroup_key, subgrp in recs.groupby(expand_columns, dropna=False):
                    perf_groups.append(
                        {
                            "Attrs": {k: subgrp.iloc[0][k] for k in expand_columns},
                            "Metrics": {
                                rec["Metric_Type"]: rec["Count"]
                                for _, rec in subgrp.iterrows()
                            },
                        }
                    )
        else:
            # date_merge is True => we store performance/metric in one object for all months
            for subgroup_key, subgrp in grp.groupby(expand_columns, dropna=False):
                metrics = {}
                perf_rec = {
                    "Attrs": {k: subgrp.iloc[0][k] for k in expand_columns},
                    "Metrics": metrics,
                }
                perf.append(perf_rec)
                for metric, recs in subgrp.groupby(["Metric_Type"], dropna=False):
                    metrics[metric] = {
                        rec["Begin_Date"]: rec["Count"] for _, rec in recs.iterrows()
                    }

        item["Performance"] = perf
        items.append(item)
    return items

def process_one_file(filename: str, date_merge: bool =False, stdout:bool=False):
    reader = C5Reader()
    with open(filename, "r") as infile:
        data = json.load(infile)
    orig_size = len(json.dumps(data))
    orig_mem = total_size(data)

    header, df = reader.json_to_header_and_df(data)
    months = len(df['Begin_Date'].unique())

    new_items = df_to_items(
        df,
        ["YOP", "Access_Type", "Access_Method", "Section_Type"],
        date_merge=args.date_merge,
    )

    out = {"Report_Header": header, "Report_Items": new_items}
    dump = json.dumps(out, ensure_ascii=False)
    ratio = len(dump) / orig_size

    new_mem = total_size(out)
    ratio_mem = new_mem / orig_mem
    print(
        f"{filename[:14]:14s} |{months:7d} |{orig_size:13d} |{len(dump):13d} |{ratio:-13.4f} |{orig_mem:13d} |{new_mem:13d} | "
        f"{ratio_mem:-12.4f}",
        file=sys.stderr,
    )

    if stdout:
        print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("filename", nargs="+", help="json file to process")
    parser.add_argument(
        "-d",
        dest="date_merge",
        action="store_true",
        help="Merge data for different months into one object",
    )
    parser.add_argument(
        "-c", dest="stdout", action="store_true", help="Output converted file to stdout"
    )

    args = parser.parse_args()

    print(
        "File           | Months |    Size orig |     Size new |   Size ratio |  Memory orig |   Memory new | Memory ratio",
        file=sys.stderr,
    )

    futures = set()
    process_pool = ProcessPoolExecutor(max_workers=6)
    for filename in args.filename:
        futures.add(process_pool.submit(process_one_file, filename, date_merge=args.date_merge, stdout=args.stdout))
    done = set()
    while futures or done:
        done, futures = wait(futures, timeout=1)
