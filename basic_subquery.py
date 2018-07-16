#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
- movie to genre is many to many
- favorite_movie_id to user_id is one to many
"""

from __future__ import print_function, division
import random
from sfm import rnd

import sqlalchemy
from sqlalchemy import Table, Column
from sqlalchemy import String, Integer, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from learn_sql_in_python.util import pprint, preview

engine = sqlalchemy.create_engine("sqlite:///:memory:")

Base = declarative_base()

movie_and_genre = Table(
    "movie_and_genre", Base.metadata,
    Column("movie_id", Integer, ForeignKey("movie.id"), primary_key=True),
    Column("genre_id", Integer, ForeignKey("genre.id"), primary_key=True),
)


class Movie(Base):
    __tablename__ = "movie"

    id = Column(Integer, primary_key=True)
    title = Column(String)
    genres = relationship("Genre", secondary=movie_and_genre,
                          back_populates="movies")


class Genre(Base):
    __tablename__ = "genre"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    movies = relationship("Movie", secondary=movie_and_genre,
                          back_populates="genres")


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    favorite_movie_id = Column(Integer, ForeignKey("movie.id"))


class Rate(Base):
    __tablename__ = "rate"

    user_id = Column(Integer, ForeignKey("user.id"), primary_key=True)
    movie_id = Column(Integer, ForeignKey("movie.id"), primary_key=True)
    score = Column(Integer)


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

genre_list = [
    Genre(id=1, name="Action"),
    Genre(id=2, name="Biography"),
    Genre(id=3, name="Crime"),
    Genre(id=4, name="Drama"),
]

n_movie = 30
movie_list = [
    Movie(
        id=id,
        title=rnd.rand_str(32),
        genres=random.sample(genre_list, random.randint(2, 4)),
    )
    for id in range(1, 1 + n_movie)
]

n_user = 6
user_list = [
    User(id=id, name=rnd.name(), favorite_movie_id=random.randint(1, n_movie))
    for id in range(1, 1 + n_user)
]

rate_list = list()
for user in user_list:
    sub_movie_list = random.sample(movie_list, random.randint(3, 10))
    sub_movie_list = list(sorted(sub_movie_list, key=lambda x: x.id))
    for movie in sub_movie_list:
        rate = Rate(
            user_id=user.id,
            movie_id=movie.id,
            score=random.randint(1, 5),
        )
        rate_list.append(rate)

ses.add_all(genre_list)
ses.add_all(movie_list)
ses.add_all(user_list)
ses.add_all(rate_list)
ses.commit()

query = sqlalchemy.text("""
SELECT rate.user_id, rate.movie_id, rate.score
FROM rate
WHERE user_id = 1
""")
pprint(query, engine)

query = sqlalchemy.text("""
SELECT rate.user_id, rate.movie_id, rate.score + (
    SELECT score
    FROM rate
    WHERE user_id = 1
) as final
FROM rate
WHERE user_id = 1
""")
pprint(query, engine)

"""
请注意上面的SQL语句, SELECT部分中间有一项是 rate.score + (subquery).
如果subquery的结果是单个数值的话, 我们很好理解. 如果subquery的结果是一个列表, 那么sql的行为又是什么呢?

经过试验返现, 无论subquery的结果有多少个, 只使用第一个数值进行计算.
"""
