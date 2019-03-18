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
    dict(id=1, salary=5000),
    dict(id=2, salary=5000),
    dict(id=3, salary=4000),
]

pg.add_table(metadata, t_employees, t_employees_data)


def row_number_example():
    sql = """
    SELECT
        E.id,
        row_number() OVER (ORDER BY E.salary DESC)
    FROM employees E
    """
    pg.print_result(sql)


row_number_example()