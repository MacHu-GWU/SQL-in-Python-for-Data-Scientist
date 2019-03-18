# -*- coding: utf-8 -*-

from learn_sql_in_python import sa, connect_local_postgres_in_container, Playground

engine = connect_local_postgres_in_container()
pg = Playground(engine)

metadata = sa.MetaData()
t_users = sa.Table(
    "users", metadata,
    sa.Column("id", sa.Integer),
    sa.Column("name", sa.String),
)

t_users_data = [
    dict(id=1, name="Alice"),
    dict(id=1, name="Bob"),
    dict(id=2, name="Cathy"),
    dict(id=2, name="David"),
]

pg.add_table(metadata, t_users, t_users_data)

sql = """
SELECT
    DISTINCT *
FROM users
"""
pg.print_result(sql)