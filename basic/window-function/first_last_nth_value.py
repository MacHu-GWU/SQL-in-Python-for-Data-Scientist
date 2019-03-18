# -*- coding: utf-8 -*-

"""
请特别注意 last_value, nth_value 的行为. last_value 并不是指整个 Window Frame 中的最后一个,
而是指当前已展开的所有 Rows 中的最后一个. 也就是说, 这个值并不会是永远相同的. nth_value 同理.
"""

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
    dict(id=2, salary=5000),
    dict(id=3, salary=4000),
    dict(id=4, salary=4000),
    dict(id=5, salary=4500),
    dict(id=6, salary=2000),
]

pg.add_table(metadata, t_employees, t_employees_data)


def first_last_nth_value_example():
    sql = """
    SELECT
        E.id,
        E.salary,
        first_value(E.id) OVER (ORDER BY E.salary DESC),
        last_value(E.id) OVER (ORDER BY E.salary DESC),
        nth_value(E.id, 2) OVER (ORDER BY E.salary DESC)
    FROM employees E
    """
    pg.print_result(sql)


first_last_nth_value_example()
