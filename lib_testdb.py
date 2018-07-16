#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
- movie to genre is many to many
- favorite_movie_id to user_id is one to many
"""

from __future__ import print_function, division
import rolex
import random
from sfm import rnd

import sqlalchemy
from sqlalchemy import Table, Column
from sqlalchemy import String, Integer, Date, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base

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


class History(Base):
    __tablename__ = "history"

    user_id = Column(Integer, ForeignKey("user.id"), primary_key=True)
    movie_id = Column(Integer, ForeignKey("movie.id"), primary_key=True)
    watched_at = Column(Date, primary_key=True)


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

history_list = list()
for user in user_list:
    for _ in range(random.randint(3, 10)):
        history = History(
            user_id=user.id,
            movie_id=random.randint(1, n_movie),
            watched_at=rolex.rnd_date("2017-01-01", "2017-01-31"),
        )
        history_list.append(history)

ses.add_all(genre_list)
ses.add_all(movie_list)
ses.add_all(user_list)
ses.add_all(history_list)
ses.commit()


