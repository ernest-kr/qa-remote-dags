from datetime import datetime

from airflow.decorators import dag, task, task_group


@dag(dag_id="taskgroup_branching_issue", schedule=None, start_date=datetime(2021, 1, 1), tags=["taskmap"])
def BranchingIssue():
    @task
    def branch_b():
        pass

    @task
    def branch_a():
        pass

    @task
    def initiate_dynamic_mapping():
        import random

        random_len = random.randint(1, 10)
        return list(range(random_len))

    @task.branch
    def branch_int(k):
        import time

        branch = "showcase_branching_issues."
        if k % 2 == 0:
            branch += "branch_a"
        else:
            time.sleep(5)
            branch += "branch_b"
        return branch

    @task_group
    def showcase_branching_issues(k):
        selected_branch = branch_int(k)
        selected_branch >> [branch_a(), branch_b()]

    list_k = initiate_dynamic_mapping()
    showcase_branching_issues.expand(k=list_k)


dag = BranchingIssue()
