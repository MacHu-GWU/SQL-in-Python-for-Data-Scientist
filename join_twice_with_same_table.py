#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
- DISTINCT function
- SELECT in WHERE clause
"""

from __future__ import print_function, division
import rolex
from sfm import rnd

import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import String, Integer, Date, ForeignKey
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.ext.declarative import declarative_base
from util import pprint

engine = sqlalchemy.create_engine("sqlite:///:memory:")

Base = declarative_base()


class Match(Base):
    __tablename__ = "match"

    id = Column(Integer, primary_key=True)
    home_team_id = Column(Integer, ForeignKey("team.id"))
    away_team_id = Column(Integer, ForeignKey("team.id"))
    time = Column(Date)


class Team(Base):
    __tablename__ = "team"

    id = Column(Integer, primary_key=True)
    name = Column(String)


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

start_date = "2017-01-01"
end_date = "2017-01-07"

team_list = [
    Team(id=1, name=rnd.name()),
    Team(id=2, name=rnd.name()),
    Team(id=3, name=rnd.name()),
]

match_list = [
    Match(id=1, home_team_id=1, away_team_id=2, time=rolex.rnd_date()),
    Match(id=2, home_team_id=2, away_team_id=3, time=rolex.rnd_date()),
]

ses.add_all(team_list)
ses.add_all(match_list)
ses.commit()


def example():
    """
    join Team.id column twice.
    """
    home = aliased(Team)
    away = aliased(Team)

    query = ses.query(Team)
    pprint(query, engine)

    query = ses.query(
                Match.id,
                Match.home_team_id,
                home.name.label("home_team"),
                Match.away_team_id,
                away.name.label("away_team"),
            ) \
        .join(home, Match.home_team_id == home.id) \
        .join(away, Match.away_team_id == away.id)
    pprint(query, engine)

    print(query)

example()
