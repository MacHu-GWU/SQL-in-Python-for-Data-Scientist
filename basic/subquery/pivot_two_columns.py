# -*- coding: utf-8 -*-

"""
Convert::

    +------------+-------+----------+
    | department | level | n_people |
    +------------+-------+----------+
    |     HR     |   2   |    1     |
    |  Finance   |   3   |    1     |
    |  Finance   |   2   |    1     |
    |     IT     |   1   |    2     |
    |     IT     |   2   |    1     |
    +------------+-------+----------+

To::

    +------------+--------+--------+--------+
    | department | n_lvl1 | n_lvl2 | n_lvl3 |
    +------------+--------+--------+--------+
    |  Finance   |   0    |   1    |   1    |
    |     HR     |   0    |   1    |   0    |
    |     IT     |   2    |   1    |   0    |
    +------------+--------+--------+--------+
"""

import sqlalchemy as sa
from sqlalchemy_mate.pt import from_resultproxy
from learn_sql_in_python.db import connect_local_postgres_in_container, reset_db

engine = connect_local_postgres_in_container()
reset_db(engine)

metadata = sa.MetaData()
t_users = sa.Table(
    "users", metadata,
    sa.Column("id", sa.Integer),
    sa.Column("name", sa.String),
    sa.Column("department", sa.String),
    sa.Column("level", sa.Integer),
)
metadata.create_all(engine)

users_data = [
    dict(id=1, name="Alice", department="HR", level=2),
    dict(id=2, name="Bob", department="Finance", level=2),
    dict(id=3, name="Cathy", department="Finance", level=3),
    dict(id=4, name="David", department="IT", level=1),
    dict(id=5, name="Elan", department="IT", level=1),
    dict(id=6, name="Fisher", department="IT", level=2),
]
engine.execute(t_users.insert(), users_data)

stmt = sa.text("""
SELECT
    T.department AS department,
    SUM(CASE WHEN T.level=1 THEN T.n_people ELSE 0 END) AS n_lvl1,
    SUM(CASE WHEN T.level=2 THEN T.n_people ELSE 0 END) AS n_lvl2,
    SUM(CASE WHEN T.level=3 THEN T.n_people ELSE 0 END) AS n_lvl3
FROM (
    SELECT
        users.department AS department,
        users.level AS level,
        COUNT(*) AS n_people
    FROM users
    GROUP BY department, level
) T
GROUP BY department
ORDER BY department
""")
result = engine.execute(stmt)
print(from_resultproxy(result))
