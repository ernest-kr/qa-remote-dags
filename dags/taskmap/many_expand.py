from dataclasses import dataclass
from datetime import datetime
from random import randint, seed

from airflow.decorators import task
from airflow.sdk import DAG
from airflow.utils.task_group import TaskGroup
from dataclasses_json import dataclass_json


# I've seen, but been unable to recreate, a bug were data in a dataclass was getting truncated part way through a string
# so lets make some string data and see if any of it gets lost
@dataclass_json
@dataclass
class Payload:
    seed: int
    a: str
    b: str
    c: str
    d: str

    def make(i):
        return Payload(seed=i, a=str(i) * 5, b=str(i) * 10, c=str(i) * 15, d=str(i) * 20)

    def transform(self):
        self.a = str(int(int(self.a) / 5))
        self.b = str(int(int(self.a) / 10))
        self.c = str(int(int(self.a) / 15))
        self.d = str(int(int(self.a) / 20))


@task
def mk_payloads(i):
    return [Payload.make(j).to_dict() for j in range(1, i)]


@task
def transform(_payload):
    payload = Payload.from_dict(_payload)
    payload.transform()
    return payload.to_dict()


@task
def check_transformed(_payload):
    payload: Payload = Payload.from_dict(_payload)
    orig = Payload.make(payload.seed)
    orig.transform()

    # same transformation made to same data
    # results should be same, otherwise something got lost along the way
    assert orig == payload


minmax_map_indices_per_lane = (5, 60)
lanes = 10


with DAG(dag_id="many_expand", schedule=None, start_date=datetime(1970, 1, 1), catchup=False, tags=["taskmap"]) as dag:
    seed(42)  # be deterministic

    for i in range(lanes):
        with TaskGroup(group_id=f"lane{i+1}"):
            min_map, max_map = minmax_map_indices_per_lane
            mapped = randint(min_map, max_map)
            check_transformed.expand(_payload=transform.expand(_payload=mk_payloads(mapped)))
