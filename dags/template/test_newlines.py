from datetime import datetime
from textwrap import dedent

from airflow.operators.smooth import SmoothOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    "test_newlines",
    schedule=None,
    doc_md=dedent(
        """
        This dag ensures that newlines are handled appropriately.
        Some of the tasks currently fail due to bugs in airflow, but they'll pass when those bugs are fixed.
        Here's the issue for the bugs: https://github.com/apache/airflow/issues/28280
        """
    ),
):
    test_bash_operator_multiline_env_var = BashOperator(
        task_id="test_bash_operator_multiline_env_var",
        start_date=datetime.now(),
        env={
            "MULTILINE_ENV_VAR": """Line 1
Line 2
Line 3"""
        },
        append_env=True,
        bash_command="""
if [[ "$(echo $MULTILINE_ENV_VAR)" != "$(echo $MULTILINE_ENV_VAR | dos2unix)" ]]; then
    echo >&2 "Multiline environment variable contains newlines incorrectly converted to Windows CRLF"
    exit 1
fi""",
    )

    test_bash_operator_heredoc_contains_newlines = BashOperator(
        task_id="test_bash_operator_heredoc_contains_newlines",
        start_date=datetime.now(),
        bash_command="""
diff <(
cat <<EOF
Line 1
Line 2
Line 3
EOF
) <(
cat | dos2unix <<EOF
Line 1
Line 2
Line 3
EOF
) || {
    echo >&2 "Bash heredoc contains newlines incorrectly converted to Windows CRLF"
    exit 1
}
""",
    )

    test_bash_operator_env_var_from_variable_jinja_interpolation = BashOperator(
        task_id="test_bash_operator_env_var_from_variable_jinja_interpolation",
        start_date=datetime.now(),
        env={
            "ENV_VAR_AIRFLOW_VARIABLE_WITH_NEWLINES": "{{ var.value['newlines'] }}",
        },
        append_env=True,
        bash_command="""
diff <(echo "$ENV_VAR_AIRFLOW_VARIABLE_WITH_NEWLINES") <(echo "$ENV_VAR_AIRFLOW_VARIABLE_WITH_NEWLINES" | dos2unix) || {
    echo >&2 "Environment variable contains newlines incorrectly converted to Windows CRLF"
    exit 1
}
""",
    )

    test_bash_operator_from_variable_jinja_interpolation = BashOperator(
        task_id="test_bash_operator_from_variable_jinja_interpolation",
        start_date=datetime.now(),
        bash_command="""
diff <(echo "{{ var.value['newlines'] }}") <(echo "{{ var.value['newlines'] }}" | dos2unix) || {
    echo >&2 "Jinja interpolated string contains newlines incorrectly converted to Windows CRLF"
    exit 1
}
""",
    )

    test_bash_operator_backslash_n_not_equals_backslash_r = BashOperator(
        task_id="test_bash_operator_backslash_n_not_equals_backslash_r",
        start_date=datetime.now(),
        bash_command="""
if [[ "\r" == "\n" ]]; then
    echo >&2 "Backslash-r has been incorrectly converted into backslash-n"
    exit 1
fi""",
    )

    passing = SmoothOperator(task_id="passing", start_date=datetime.now())
    failing = SmoothOperator(task_id="failing", start_date=datetime.now())

    [
        test_bash_operator_multiline_env_var,
        test_bash_operator_heredoc_contains_newlines,
    ] >> passing
    [
        test_bash_operator_env_var_from_variable_jinja_interpolation,
        test_bash_operator_from_variable_jinja_interpolation,
        test_bash_operator_backslash_n_not_equals_backslash_r,
    ] >> failing
