# -*- coding: utf-8 -*-

from learn_sql_in_python import sa, connect_local_postgres_in_container, Playground

engine = connect_local_postgres_in_container()
pg = Playground(engine)
metadata = sa.MetaData()
t_employees = sa.Table(
    "employees", metadata,
    sa.Column("id", sa.Integer),
    sa.Column("salary", sa.Integer),
)

t_employees_data = [
    dict(id=1, salary=3000),
    dict(id=2, salary=4000),
    dict(id=3, salary=5000),
    dict(id=4, salary=1000),
    dict(id=5, salary=2000),
]

pg.add_table(metadata, t_employees, t_employees_data)


def find_second_highest_salary_employee():
    sql = """
    SELECT
        T.id AS id,
        T.salary AS salary
    FROM
    (
        SELECT
            employees.id AS id,
            employees.salary AS salary,
            rank() OVER (ORDER BY employees.salary DESC) AS nth
        FROM employees
    ) T
    WHERE T.nth = 4
    """
    pg.print_result(sql)


# find_second_highest_salary_employee()

def find_top3_highest_salary_employee():
    sql = """
    SELECT
        E1.id AS id
    FROM 
        employees E1,
        employees E2
    WHERE 
        E1.salary <= E2.salary
    GROUP BY E1.id
    HAVING COUNT(E1.id) <= 3
    """
    pg.print_result(sql)


find_top3_highest_salary_employee()
