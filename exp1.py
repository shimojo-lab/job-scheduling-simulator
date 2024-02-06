import pandas as pd
from modules.simulator import Simulator
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("method", type=int)
args = parser.parse_args()

DEBUG = False

NODE_SIZE = 1000
SCHEDULE_TIMESTEP_WINDOW = 2880
BACKFILL_TIMESTEP_WINDOW = 1440
WATCH_JOB_SIZE = 300
TIMESTEP_SECONDS = 60
SAMPLE_SIZE = 100000

if DEBUG:
    NODE_SIZE = 10
    SCHEDULE_TIMESTEP_WINDOW = 40
    BACKFILL_TIMESTEP_WINDOW = 30
    WATCH_JOB_SIZE = 5
    TIMESTEP_SECONDS = 60
    SAMPLE_SIZE = 100

print("*** Params ***")
print(f"Node size: {NODE_SIZE}")
print(f"Timestep window: {SCHEDULE_TIMESTEP_WINDOW}")
print(f"Backfill window: {BACKFILL_TIMESTEP_WINDOW}")
print(f"Watch job size: {WATCH_JOB_SIZE}")
print(f"Timestep seconds: {TIMESTEP_SECONDS}")
print(f"Sample size: {SAMPLE_SIZE}")
print("\n")


data_list = [
    {
        "name": "jobs-proposed-method",
        "path": "data/jobs-proposed-method.parquet",
    },
    {
        "name": "jobs-previous-method",
        "path": "data/jobs-previous-method.parquet",
    },
    {
        "name": "jobs-last2",
        "path": "data/jobs-last2.parquet",
    },
    {
        "name": "jobs-user-estimation",
        "path": "data/jobs-user-estimation.parquet",
    },
]

df_list = [pd.read_parquet(data["path"]) for data in data_list]
common_log_ids = df_list[0][["log_id", "y_pred", "ehost_num"]]
for df in df_list[1:]:
    common_log_ids = pd.merge(
        common_log_ids,
        df[["log_id", "y_pred"]],
        on="log_id",
        how="inner",
        suffixes=("", "_"),
    )

# リソースに割り当て可能なジョブに絞り込み
common_log_ids["y_pred_max"] = common_log_ids.filter(like="y_pred").max(axis=1)
common_log_ids = common_log_ids[
    common_log_ids["y_pred_max"] < SCHEDULE_TIMESTEP_WINDOW * TIMESTEP_SECONDS
]
common_log_ids = common_log_ids[common_log_ids["ehost_num"] < NODE_SIZE]

# 共通するlog_idで絞り込み
sampled_log_ids = common_log_ids.sample(SAMPLE_SIZE, random_state=1030)[["log_id"]]
df_list = [pd.merge(sampled_log_ids, df, on="log_id", how="inner") for df in df_list]

data = data_list[args.method]
df = df_list[args.method]
print(f"Running {data['name']}...")
simulator = Simulator(
    df,
    NODE_SIZE,
    SCHEDULE_TIMESTEP_WINDOW,
    BACKFILL_TIMESTEP_WINDOW,
    WATCH_JOB_SIZE,
    TIMESTEP_SECONDS,
)
simulator.run()
