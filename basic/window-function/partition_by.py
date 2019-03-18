# -*- coding: utf-8 -*-

"""
**Question**:

Compare each employee's salary with the average salary in his or her department
"""

from learn_sql_in_python import sa, connect_local_postgres_in_container, Playground

engine = connect_local_postgres_in_container()
pg = Playground(engine)

metadata = sa.MetaData()
t_employees = sa.Table(
    "employees", metadata,
    sa.Column("eid", sa.Integer),
    sa.Column("department", sa.String),
    sa.Column("salary", sa.Integer),
)
t_employees_data = [
    dict(eid=1, department="HR", salary=3500),
    dict(eid=2, department="HR", salary=2800),
    dict(eid=3, department="HR", salary=3000),
    dict(eid=4, department="IT", salary=4200),
    dict(eid=5, department="IT", salary=6000),
    dict(eid=6, department="IT", salary=5500),
    dict(eid=7, department="IT", salary=7500),
]
pg.add_table(metadata, t_employees, t_employees_data)


def compare_with_in_department_avg_salary():
    sql = """
    SELECT
        employees.department AS department,
        employees.eid AS eid,
        employees.salary AS salary,
        CAST(
            AVG(employees.salary) OVER (PARTITION BY employees.department) AS INTEGER
        ) AS avg_dept_salary
    FROM employees; 
    """
    pg.print_result(sql)


compare_with_in_department_avg_salary()
