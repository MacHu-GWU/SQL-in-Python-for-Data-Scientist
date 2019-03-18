# -*- coding: utf-8 -*-

"""
Keyword: sub query, subquery

Sometime we want to cache result of a query, and do a lots of operation on that
subquery.
"""

import random
import sqlalchemy as sa
from sqlalchemy_mate.pt import from_resultproxy
from learn_sql_in_python.db import connect_local_postgres_in_container, reset_db

engine = connect_local_postgres_in_container()
reset_db(engine)

metadata = sa.MetaData()
t_tags = sa.Table(
    "tags", metadata,
    sa.Column("movie_id", sa.Integer),
    sa.Column("tag_id", sa.Integer),
)
metadata.create_all(engine)

n_movie = 100
n_tag = 20
tag_id_list = list(range(1, n_tag + 1))
tags_data = list()
for movie_id in range(1, n_movie + 1):
    for tag_id in random.sample(tag_id_list, random.randint(2, 4)):
        tags_data.append({"movie_id": movie_id, "tag_id": tag_id})
engine.execute(t_tags.insert(), tags_data)

# 选出包含 tag1, 但是不包含 tag2 的 data
with engine.connect() as conn:
    stmt = sa.text("""
    CREATE TEMP TABLE temp_tags(movie_id INT, tag_id INT)
    """)
    conn.execute(stmt)

    stmt = sa.text("""
    INSERT INTO temp_tags
        SELECT *
        FROM tags
        WHERE tags.tag_id BETWEEN 1 AND 2
        ORDER BY tag_id
    """)
    conn.execute(stmt)

    stmt = sa.text("""
    SELECT * 
    FROM temp_tags
    """)
    print(from_resultproxy(conn.execute(stmt)))

    stmt = sa.text("""
    SELECT * 
    FROM
    (
        (
            SELECT temp_tags.movie_id
            FROM temp_tags
            WHERE temp_tags.tag_id = 1
        )
        EXCEPT
        (
            SELECT temp_tags.movie_id
            FROM temp_tags
            WHERE temp_tags.tag_id = 2
        )
    ) T
    """)
    print(from_resultproxy(conn.execute(stmt)))
