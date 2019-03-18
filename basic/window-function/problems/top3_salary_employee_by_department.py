# -*- coding: utf-8 -*-

"""

"""

from learn_sql_in_python import sa, connect_local_postgres_in_container, Playground

engine = connect_local_postgres_in_container()
pg = Playground(engine)

metadata = sa.MetaData()
t_employees = sa.Table(
    "employees", metadata,
    sa.Column("id", sa.Integer),
    sa.Column("dept_id", sa.Integer),
    sa.Column("salary", sa.Integer),
)

t_employees_data = [
    dict(id=1, dept_id=1, salary=5000),
    dict(id=2, dept_id=1, salary=6500),
    dict(id=3, dept_id=1, salary=7000),
    dict(id=4, dept_id=1, salary=7500),
    dict(id=5, dept_id=1, salary=4000),
    dict(id=6, dept_id=2, salary=4000),
    dict(id=7, dept_id=2, salary=5000),
    dict(id=8, dept_id=2, salary=4000),
    dict(id=9, dept_id=2, salary=4500),
    dict(id=10, dept_id=2, salary=3500),
]

pg.add_table(metadata, t_employees, t_employees_data)


def top3_salary_employee_by_department():
    """
    Find top 3 salary employee for each department.

    Result::

        +---------+-------------+--------+------+
        | dept_id | employee_id | salary | rank |
        +---------+-------------+--------+------+
        |    1    |      4      |  7500  |  1   |
        |    1    |      3      |  7000  |  2   |
        |    1    |      2      |  6500  |  3   |
        |    2    |      7      |  5000  |  1   |
        |    2    |      9      |  4500  |  2   |
        |    2    |      6      |  4000  |  3   |
        |    2    |      8      |  4000  |  3   |
        +---------+-------------+--------+------+
    """
    sql = """
    SELECT *
    FROM 
    (
        SELECT
            E.dept_id AS dept_id,
            E.id AS employee_id,
            E.salary AS salary,
            rank() OVER (PARTITION BY E.dept_id ORDER BY E.salary DESC) AS rank
        FROM employees E
    ) E1
    WHERE E1.rank <= 3
    """
    pg.print_result(sql)


top3_salary_employee_by_department()
