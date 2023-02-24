import csv
import random
from typing import List

from models import Job

NODE_COUNT = 4
ROWS = 10
MAX_REQ_SEC = 10


# モックデータを生成する関数
def generate_mock_data(node_count, rows, max_req_sec):
    data = []
    for i in range(rows):
        node = random.randint(1, node_count)
        req_sec = random.randint(1, max_req_sec)
        act_sec = random.randint(1, req_sec)
        pred_sec = random.randint(1, req_sec)
        job = Job(i + 1, node, req_sec, act_sec, pred_sec)
        data.append(job)
    return data


# CSV形式でデータを出力する関数
def write_csv_data(data: List[Job]):
    with open("job_data.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "node", "req_time", "act_time", "pred_time"])
        for job in data:
            writer.writerow([job.id, job.node, job.req_sec, job.act_sec, job.pred_sec])


# モックデータを生成してCSV形式で出力する
data = generate_mock_data(NODE_COUNT, ROWS, MAX_REQ_SEC)
write_csv_data(data)
