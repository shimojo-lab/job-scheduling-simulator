import csv
import random
from typing import List

from models import Job

NODE_COUNT = 4
ROWS = 10
MAX_REQ_runtime = 10


# モックデータを生成する関数
def generate_mock_data(node_count, rows, max_req_runtime):
    data = []
    for i in range(rows):
        node = random.randint(1, node_count)
        req_runtime = random.randint(1, max_req_runtime)
        act_runtime = random.randint(1, req_runtime)
        pred_runtime = random.randint(1, req_runtime)
        job = Job(i + 1, node, req_runtime, act_runtime, pred_runtime)
        data.append(job)
    return data


# CSV形式でデータを出力する関数
def write_csv_data(data: List[Job]):
    with open("job_data.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "node", "req_runtime", "act_runtime", "pred_runtime"])
        for job in data:
            writer.writerow(
                [
                    job.id,
                    job.node_amount,
                    job.req_runtime,
                    job.act_runtime,
                    job.pred_runtime,
                ]
            )


# モックデータを生成してCSV形式で出力する
data = generate_mock_data(NODE_COUNT, ROWS, MAX_REQ_runtime)
write_csv_data(data)
