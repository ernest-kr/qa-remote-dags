# jinja version used in airflow: 2.11.3
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from jinja2.ext import loopcontrols
from pendulum import today

# jinja.Environment
"""['_compile', '_generate', '_load_template', '_parse', '_tokenize', 'add_extension', 'auto_reload', 'autoescape', 'block_end_string', 'block_start_string', 'bytecode_cache', 'cache', 'call_filter', 'call_test', 'code_generator_class', 'comment_end_string', 'comment_start_string', 'compile', 'compile_expression', 'compile_templates', 'context_class', 'enable_async', 'extend', 'extensions', 'filters', 'finalize', 'from_string', 'get_or_select_template', 'get_template', 'getattr', 'getitem', 'globals', 'handle_exception', 'is_async', 'iter_extensions', 'join_path', 'keep_trailing_newline', 'lex', 'lexer', 'line_comment_prefix', 'line_statement_prefix', 'linked_to', 'list_templates', 'loader', 'lstrip_blocks', 'make_globals', 'newline_sequence', 'optimized', 'overlay', 'overlayed', 'parse', 'policies', 'preprocess', 'sandboxed', 'select_template', 'shared', 'template_class', 'tests', 'trim_blocks', 'undefined', 'variable_end_string', 'variable_start_string']"""

# The default values for the start and end strings are '{{' and '}}' while the changed variable strings are '{[(' and ')]}' repsectively.\n
# The default values for the start and end block strings are '{%' and '%}' while the changed values are '($' and '$)'

docs = """
####Purpose
This dag checks that airflow is interfacing with jinja properly by setting a few of jinjas environment variables.
####Expected Behavior
The 1st and 2nd task is expected to succeed.\n
The 1st task asserts that airflow is setting the 'variable_start_string' and 'variable_end_string' in jinjas environment correctly.\n
The 2nd task asserts that airflow is setting the 'block_start_string' and 'block_end_string' in jinja's environment correctly.\n
"""


def check_jinja_env(ls_of_ints):
    assert ls_of_ints == list(range(11))


templated_command = """
        ($ for i in range(5) $)
            echo "{[( ds )]}"
            echo "{[( macros.ds_add(ds, 7))]}"
            echo "{[( params.my_param )]}"
        ($ endfor $)
"""

with DAG(
    dag_id="jinja_environment_kwargs",
    schedule=None,
    start_date=today("UTC").add(days=-2),
    tags=["dagparams"],
    render_template_as_native_obj=True,
    user_defined_macros={"tenner": list(range(11))},
    # additional configuration options to be passed to Jinja Environment for template rendering
    jinja_environment_kwargs={
        # default is false check rendered_templates in ui, keeps the trailing newline
        "keep_trailing_newline": True,
        # default is '{{' changes the characters for the start of jinja string variables
        "variable_start_string": "{[(",
        # default is '}}' changes the characters for the end of jinja string variables
        "variable_end_string": ")]}",
        # default is '{%' changes the characters for the start of jinja blocks
        "block_start_string": "($",
        # default is '%}' changes the characters for the end of jinja blocks
        "block_end_string": "$)",
        # list of jinja extensions to use
        # This extension adds support for break and continue in loops. After enabling, Jinja provides those two keywords which work exactly like in Python.
        "extensions": [loopcontrols],
    },
    doc_md=docs,
) as dag:
    py0 = PythonOperator(
        task_id="jinja_env_change_str_chars",
        python_callable=check_jinja_env,
        # notice the characters are different than '{{ str_var }}'
        op_args=["{[( tenner )]}"],
    )

    b0 = BashOperator(
        task_id="change_jinja_block_chars",
        bash_command=templated_command,
        params={"my_param": "the start date has been added to by 7 days"},
    )

py0 >> b0
