# -*- coding: utf-8 -*-

"""
ntile 简单来说就是将整个区间划分为 N 个整数,

- 0 ~ 1 / N percentile 的给赋值 1
- 0 ~ 2 / N percentile 的给赋值 2
- ...
"""

import random
from learn_sql_in_python import sa, connect_local_postgres_in_container, Playground

engine = connect_local_postgres_in_container()
pg = Playground(engine)

metadata = sa.MetaData()
t_employees = sa.Table(
    "employees", metadata,
    sa.Column("id", sa.Integer),
    sa.Column("salary", sa.Integer),
)

n_employees = 100
t_employees_data = [
    dict(id=id, salary=random.randint(3000, 16000))
    for id in range(1, n_employees + 1)
]

pg.add_table(metadata, t_employees, t_employees_data)


def ntile_example():
    sql = """
    SELECT
        E.id,
        E.salary,
        ntile(10) OVER (ORDER BY E.salary DESC)
    FROM employees E
    """
    pg.print_result(sql)


ntile_example()
