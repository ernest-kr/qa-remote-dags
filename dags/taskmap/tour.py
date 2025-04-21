from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# You can send data to operator kwargs from separate tasks.  If you do it
# this way (expand instead of using expand_kwargs, as shown below)
# you'll get N * M tasks, where N and M are however many items each
# input task came up with. In this case, that means four task instances.
#
# This functionality was in the 2.3 release

with DAG(
    dag_id="expand_product",
    doc_md="use expand and get a cross product (4 tasks)",
    schedule=timedelta(days=30 * 365),
    start_date=datetime(1970, 1, 1),
    tags=["taskmap"],
) as expand_product:
    # these are hard coded, but they're evaluated at runtime
    # so they could easily be dynamic, like the result of a
    # SQL query or something like that.
    @task
    def get_cmd():
        return ["echo hello $USER", "echo goodbye $USER"]

    @task
    def get_env():
        return [{"$USER": "foo"}, {"$USER": "bar"}]

    BashOperator.partial(
        task_id="four_cmds",
    ).expand(env=get_env(), bash_command=get_cmd())


with DAG(
    dag_id="expand_product_tf",
    doc_md="use expand and get a cross product with the taskflow interface (4 tasks)",
    schedule=timedelta(days=30 * 365),
    start_date=datetime(1970, 1, 1),
    tags=["taskmap"],
) as expand_product_tf:

    @task
    def get_template():
        return ["hello {}", "goodbye {}"]

    @task
    def get_var():
        return ["foo", "bar"]

    @expand_product_tf.task
    def salutations(template, user):
        print(template.format(user))

    salutations.partial().expand(template=get_template(), user=get_var())

# Like above, these two dags map some "dynamic" data into tasks, both
# with and without the taskflow interface.
#
# What's different is that these dags use expand_kwargs, which is new.
# Instead of using separate tasks as data sources and generating
# dynamic tasks based on the product of their output, expand_kwargs
# lets you use a single task as a data source and divide parts of its
# output to different kwargs.
#
# This means that you can have the task return a list of N items and
# you'll get N tasks.

with DAG(
    dag_id="expand_kwargs",
    doc_md="use expand_kwargs without a function",
    schedule=timedelta(days=30 * 365),
    start_date=datetime(1970, 1, 1),
    tags=["taskmap"],
) as expand_kwargs:

    @task
    def data():
        return [
            {"bash_command": "echo hello $USER", "env": {"USER": "foo"}},
            {"bash_command": "echo goodbye $USER", "env": {"USER": "bar"}},
        ]

    BashOperator.partial(task_id="two_tasks").expand_kwargs(data())

with DAG(
    dag_id="expand_kwargs_tf",
    doc_md="use expand_kwargs with the taskflow interface and without a function",
    schedule=timedelta(days=30 * 365),
    start_date=datetime(1970, 1, 1),
    tags=["taskmap"],
) as expand_kwargs_tf:

    @task
    def data():
        return [
            {"template": "hello {}", "user": "foo"},
            {"template": "goodbye {}", "user": "bar"},
        ]

    @task(task_id="two_tasks")
    def printer(template, user):
        print(template.format(user))

    printer.expand_kwargs(data())


# Like above, the two dags below map data into task instances, but
# in this case, the data is not in the right "shape" to "fit" into
# the operators that we're using.  To handle this, we provide a
# function which:
# - transforms the data into a kwarg dict if a task instance should
#   be created
# - raises AirflowSkipException of no task instance should be created


with DAG(
    dag_id="expand_kwargs_mod",
    doc_md="use expand_kwargs with a mapper function",
    schedule=timedelta(days=30 * 365),
    start_date=datetime(1970, 1, 1),
    tags=["taskmap"],
) as expand_kwargs_mod:

    @task
    def data():
        return [
            ("echo hello $USER", "USER", "foo"),
            ("echo hello $USER", "USER", "bar"),
        ]

    def mapper(entry):
        if entry[2] != "bar":
            return {"bash_command": entry[0], "env": {entry[1]: entry[2]}}
        else:
            raise AirflowSkipException("no entry for foo")

    BashOperator.partial(task_id="one_task").expand_kwargs(data().map(mapper))

with DAG(
    dag_id="expand_kwargs_mod_tf",
    doc_md="use expand_kwargs with a mapper function and the taskflow interface",
    schedule=timedelta(days=30 * 365),
    start_date=datetime(1970, 1, 1),
    tags=["taskmap"],
) as expand_kwargs_mod_tf:

    @task
    def data():
        return [
            ("hello {}", "foo"),
            ("goodbye {}", "bar"),
        ]

    def mapper(entry):
        if entry[1] != "bar":
            return {"template": entry[0], "user": entry[1]}
        else:
            raise AirflowSkipException("no entry for foo")

    @task(task_id="one_task")
    def printer(template, user):
        print(template.format(user))

    printer.expand_kwargs(data().map(mapper))


# Another new AIP-42-related addition has to do with "zipping" xcom data together
# this lets us take separate sources of data and combine them into a single list

with DAG(
    dag_id="zip",
    doc_md="zip two datasets together (2 tasks)",
    schedule=timedelta(days=30 * 365),
    start_date=datetime(1970, 1, 1),
    tags=["taskmap"],
) as zip:

    @task
    def get_cmd():
        return ["echo hello $VAR", "echo goodbye $VAR"]

    @task
    def get_env():
        return [{"VAR": "foo"}, {"VAR": "bar"}]

    def mapper(arg):
        return {"bash_command": arg[0], "env": arg[1]}

    combined = get_cmd().zip(get_env()).map(mapper)

    BashOperator.partial(
        task_id="two_cmds",
    ).expand_kwargs(combined)


with DAG(
    dag_id="zip_tf",
    doc_md="zip two datasets together using the taskflow interface (2 tasks)",
    schedule=timedelta(days=30 * 365),
    start_date=datetime(1970, 1, 1),
    tags=["taskmap"],
) as zip_tf:

    @task
    def get_template():
        return ["hello {}", "goodbye {}"]

    @task
    def get_user():
        return ["foo", "bar"]

    @task
    def salutations(arg):
        print(arg[0].format(arg[1]))

    # join separate data sources together
    combined = get_template().zip(get_user())
    salutations.expand(arg=combined)
