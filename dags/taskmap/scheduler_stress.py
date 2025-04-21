import gzip
import os
from datetime import datetime, timedelta
from random import choices, randint, seed
from string import ascii_letters
from tempfile import TemporaryDirectory
from time import sleep, time

from airflow.decorators import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from pendulum import today

# how much work to generate
height = 100

# how much parallelism
depth = 10

# how long can each step be?  (100 ~ 10 sec)
max_hardness = 30

# how many different workloads to randomly assign?
variation_types = 15
variation_count = 3


# determines the outcome of pseudorandom choices like whether to sleep or work, and for how long
# hard code a value here for (more or less) deterministic timing
# shape_seed = randint(1, 1000)
shape_seed = 42


class Directive:
    def sleep_step(n):
        """
        sleep for an amount of time determined by n
        n == 100 --> 10 seconds
        """

        print(f"sleeping {n}")
        sleep(n**2 * 0.01)
        print("    done")

    def work_step(n):
        """
        work for an amount of time determined by n
        n == 100 --> 10 seconds, more or less
        """

        print(f"working {n}")
        with TemporaryDirectory() as dir:
            fname = os.path.join(dir, "rand")
            with open(fname, "wb") as f:
                bits = "".join(choices(ascii_letters, k=(8000 * n**2))).encode("utf-8")
                compressed = gzip.compress(bits, compresslevel=3)
                f.write(compressed)
            with gzip.open(fname) as f:
                f.read()
        print("    done")

    def sleep_until_10_step(_):
        """
        sleep until the next multiple of 10 seconds
        """
        duration = (-datetime.now().second) % 10
        target = datetime.now() + timedelta(seconds=duration)
        print(f"sleeping until {target.strftime(r'%H:%M:%S')}")
        sleep(duration)
        print("done")

    def __init__(self, n):
        """
        Deterministically come up with some steps to take
        """

        seed(n)
        self.steps = []

        which_type = randint(1, 2)

        if which_type == 1:
            step_numbers = range(randint(1, variation_count))
            for _ in step_numbers:
                how_hard = randint(0, max_hardness)
                if randint(0, 1) == 0:
                    self.steps.append((Directive.sleep_step, how_hard))
                else:
                    self.steps.append((Directive.work_step, how_hard))
        else:
            self.steps = [(Directive.sleep_until_10_step, None)]

    def walk_steps(self):
        for do_this, like_this in self.steps:
            do_this(like_this)


@task
def report_params(seed):
    print("height", height)
    print("depth", depth)
    print("max_hardness", max_hardness)
    print("variation_types", variation_types)
    print("variation_count", variation_count)
    print("seed", seed)


def busy_worker(name):
    @task(task_id=name)
    def busy_work(seed):
        before = time()
        work = Directive(seed)
        work.walk_steps()
        duration = time() - before
        print("duration:", duration)

    return busy_work


@dag(schedule=None, start_date=today("UTC").add(days=-1), catchup=False, tags=["taskmap_neg"])
def scheduler_stress_withoutexpand():
    # report the seed for this execution
    start = report_params(shape_seed)
    seed(shape_seed)

    for child_num in range(1, randint(2, 2 * height)):
        child = EmptyOperator(task_id=f"child_{child_num}")

        for grandchild_num in range(1, randint(2, 2 * depth)):
            child >> busy_worker(f"grandchild_{child_num}_{grandchild_num}")(randint(1, variation_types))

        start >> child


@dag(schedule=None, start_date=today("UTC").add(days=-1), catchup=False, tags=["taskmap_neg"])
def scheduler_stress_withexpand():
    # report the seed for this execution
    start = report_params(shape_seed)
    seed(shape_seed)

    for child_num in range(1, randint(2, 2 * height)):
        grandchild_seeds = []
        for _ in range(1, randint(2, 2 * depth)):
            grandchild_seeds.append(randint(1, variation_types))

        start >> busy_worker(f"child_{child_num}").expand(seed=grandchild_seeds)


the_dag = scheduler_stress_withexpand()
the_dag2 = scheduler_stress_withoutexpand()
