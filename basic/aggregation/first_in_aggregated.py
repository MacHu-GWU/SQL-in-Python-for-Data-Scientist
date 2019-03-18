# -*- coding: utf-8 -*-

from datetime import datetime
from learn_sql_in_python import sa, connect_local_postgres_in_container, Playground

engine = connect_local_postgres_in_container()
pg = Playground(engine)

metadata = sa.MetaData()
t_sessions = sa.Table(
    "sessions", metadata,
    sa.Column("id", sa.Integer),
    sa.Column("name", sa.String),
    sa.Column("time", sa.DateTime),
)

t_sessions_data = [
    dict(id=1, name="Homepage", time=datetime(2017, 1, 1)),
    dict(id=1, name="Signup", time=datetime(2017, 1, 2)),
    dict(id=1, name="Signin", time=datetime(2017, 1, 3)),
    dict(id=2, name="Homepage", time=datetime(2017, 1, 3)),
    dict(id=2, name="Signup", time=datetime(2017, 1, 2)),
    dict(id=2, name="Signin", time=datetime(2017, 1, 1)),
]

pg.add_table(metadata, t_sessions, t_sessions_data)

sql = """
SELECT
    COUNT(DISTINCT(id))
FROM 
(
    SELECT
        S.id,
        S.name,
        rank() OVER (PARTITION BY S.id ORDER BY S.time ASC) AS rank
    FROM sessions S
) S1
WHERE 
    S1.rank = 1
    AND S1.name = 'Homepage'
"""
pg.print_result(sql)