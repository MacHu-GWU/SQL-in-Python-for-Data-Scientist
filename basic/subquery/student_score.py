# -*- coding: utf-8 -*-

"""
**Question**:

Find all student that all classes's score is greater than 80.
"""

import sqlalchemy as sa
from sqlalchemy_mate.pt import from_resultproxy
from learn_sql_in_python.db import connect_local_postgres_in_container, reset_db

engine = connect_local_postgres_in_container()
reset_db(engine)

metadata = sa.MetaData()
t_scores = sa.Table(
    "scores", metadata,
    sa.Column("sid", sa.Integer),
    sa.Column("classname", sa.String),
    sa.Column("score", sa.Integer),
)
metadata.create_all(engine)

scores_data = [
    dict(sid=1, classname="Math", score=65),
    dict(sid=1, classname="Music", score=85),
    dict(sid=1, classname="History", score=70),
    dict(sid=2, classname="Math", score=95),
    dict(sid=2, classname="Music", score=85),
    dict(sid=2, classname="History", score=90),
    dict(sid=3, classname="Math", score=88),
    dict(sid=3, classname="History", score=92),
]
engine.execute(t_scores.insert(), scores_data)

stmt = sa.text("""
SELECT scores.sid as sid
FROM scores
GROUP BY sid
HAVING MIN(scores.score) >= 80
ORDER BY sid
""")
print(from_resultproxy(engine.execute(stmt)))
