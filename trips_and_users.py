#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
- Join a table twice
-
"""

from __future__ import print_function, division
import random
from sfm import rnd
import rolex
import numpy as np

import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import String, Integer, Date, Boolean, ForeignKey
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import func, text
from util import pprint

engine = sqlalchemy.create_engine("sqlite:///:memory:")

Base = declarative_base()


class Trip(Base):
    __tablename__ = "trip"

    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("user.id"))
    driver_id = Column(Integer, ForeignKey("user.id"))
    status_id = Column(Integer, ForeignKey("status.id"))
    request_at = Column(Date)


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    banned = Column(Boolean)
    role_id = Column(Integer, ForeignKey("role.id"))


class Status(Base):
    __tablename__ = "status"

    id = Column(Integer, primary_key=True)
    name = Column(String)


class Role(Base):
    __tablename__ = "role"

    id = Column(Integer, primary_key=True)
    name = Column(String)


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

n_user = 500
n_client_perc = 0.8
banned_prob = 0.1
need_trip_prob = 0.33

start_date = "2017-01-01"
end_date = "2017-01-07"

status_list = [
    Status(id=1, name="completed"),
    Status(id=2, name="cancelled_by_driver"),
    Status(id=3, name="cancelled_by_client"),
]

role_list = [
    Role(id=1, name="client"),
    Role(id=2, name="driver"),
]

user_list = list()
client_id_list = list()
driver_id_list = list()
for id in range(1, 1 + n_user):
    flag = random.random()

    if flag > n_client_perc:
        role_id = 2
        driver_id_list.append(id)
    else:
        role_id = 1
        client_id_list.append(id)

    if flag > banned_prob:
        banned = False
    else:
        banned = True

    user = User(
        id=id,
        name=rnd.name(),
        banned=banned,
        role_id=role_id,
    )
    user_list.append(user)

trip_list = list()
for today in rolex.time_series(start_date, end_date, freq="1day", return_date=True):
    for client_id in client_id_list:
        flag = random.random()
        if flag < need_trip_prob:
            trip = Trip(
                client_id=client_id,
                driver_id=random.choice(driver_id_list),
                status_id=int(np.random.choice([1, 2, 3], size=1, p=[0.7, 0.2, 0.1])[0]),
                request_at=today,
            )
            trip_list.append(trip)

ses.add_all(status_list)
ses.add_all(role_list)
ses.add_all(user_list)
ses.add_all(trip_list)

ses.commit()


def example1():
    """
    find the cancellation rate of requests made by unbanned users
    between 2017-01-01 to 2017-01-03
    """
    query = ses.query(Trip).limit(5)
    pprint(query, engine)

    query = ses.query(User).limit(5)
    pprint(query, engine)

    client = aliased(User)
    driver = aliased(User)

    """
    SELECT count(trip.id) AS n_complete, trip.request_at AS trip_request_at 
    FROM trip 
        JOIN user AS user_1 
        ON trip.client_id = user_1.id 
        JOIN user AS user_2 
        ON trip.driver_id = user_2.id 
    WHERE 
        user_1.banned = 0 
        AND user_2.banned = 0 
        AND trip.status_id = ? 
        AND trip.request_at BETWEEN ? AND ? 
    GROUP BY trip.request_at
    """
    query = ses.query(
            func.count(Trip.id).label("n_complete"),
            Trip.request_at,
            # Trip.id,
            # Trip.client_id,
            # client.name.label("client_name"),
            # client.banned.label("client_banned"),
            # Trip.driver_id,
            # driver.name.label("driver_name"),
            # driver.banned.label("driver_banned"),
            # Trip.status_id,
            # Trip.request_at,
        ) \
        .join(client, Trip.client_id==client.id) \
        .join(driver, Trip.driver_id==driver.id) \
        .filter(client.banned==False) \
        .filter(driver.banned==False) \
        .filter(Trip.status_id==1) \
        .filter(Trip.request_at.between("2017-01-01", "2017-01-03")) \
        .group_by(Trip.request_at)

    pprint(query, engine)

    query = ses.query(
            func.count(Trip.id).label("n_all_trip"),
            Trip.request_at,
        ) \
        .join(client, Trip.client_id==client.id) \
        .join(driver, Trip.driver_id==driver.id) \
        .filter(client.banned==False) \
        .filter(driver.banned==False) \
        .filter(Trip.request_at.between("2017-01-01", "2017-01-03")) \
        .group_by(Trip.request_at)

    pprint(query, engine)

    query = ses.query(
            func.count(Trip.id).label("n_all_trip"),
            Trip.request_at,
        ) \
        .join(client, Trip.client_id==client.id) \
        .join(driver, Trip.driver_id==driver.id) \
        .filter(client.banned==False) \
        .filter(driver.banned==False) \
        .filter(Trip.request_at.between("2017-01-01", "2017-01-03")) \
        .group_by(Trip.request_at)

    pprint(query, engine)


example1()
