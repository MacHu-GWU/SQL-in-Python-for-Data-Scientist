#!/usr/bin/env python
# -*- coding: utf-8 -*-

from lib_testdb import (
    engine, pprint, preview,
    Movie, Genre, User, History
)
from sqlalchemy import text

preview(Movie, engine)
preview(Genre, engine)
preview(User, engine)
preview(History, engine)

query = text("""
SELECT MIN(history.movie_id)
FROM history
WHERE history.user_id = 1
""")
pprint(query, engine)