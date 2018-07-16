#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
- DISTINCT function
- SELECT in WHERE clause
"""

from __future__ import print_function, division
import rolex
import random
from sfm import rnd

import sqlalchemy
from sqlalchemy import Table, Column
from sqlalchemy import String, Integer, Date, ForeignKey
from sqlalchemy import text, func
from sqlalchemy.orm import sessionmaker, aliased, relationship
from sqlalchemy.ext.declarative import declarative_base
from util import pprint

engine = sqlalchemy.create_engine("sqlite:///:memory:")

Base = declarative_base()


movie_and_genre = Table("movie_and_genre", Base.metadata,
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
    for id in range(1, 1+n_movie)
]


ses.add_all(genre_list)
ses.add_all(movie_list)
ses.commit()


def example():
    """
    每个movie都有若干个genre. 我们想要统计对于每个genre, 所有包含这个genre的movie, 平均有多少个genre.
    """
    query = text("""
    SELECT genre.name, result.avg_genre_count
    FROM (
        SELECT movie_and_genre.genre_id, AVG(genre_count_each_movie.genre_count) as avg_genre_count
        FROM movie_and_genre
        JOIN
        (
            SELECT movie_and_genre.movie_id, COUNT(*) as genre_count
            FROM movie_and_genre
            GROUP BY movie_and_genre.movie_id
        ) as genre_count_each_movie
        ON movie_and_genre.movie_id = genre_count_each_movie.movie_id
        GROUP BY movie_and_genre.genre_id
    ) as result
    JOIN genre
    ON result.genre_id = genre.id
    """)
    pprint(query, engine)

example()