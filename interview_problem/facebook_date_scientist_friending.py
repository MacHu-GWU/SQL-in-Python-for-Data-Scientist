#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Facebook Data Scientist Interview Problems.
"""

from __future__ import print_function, division
import random
import rolex
from sfm import rnd
import itertools
from sqlalchemy_mate import select
from datetime import date

import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import String, Integer, Date, Boolean, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import func

from learn_sql_in_python.db import sqlite_in_memory_engine as engine
# from learn_sql_in_python.db import postgres_engine as engine
from learn_sql_in_python.util import pprint, preview

Base = declarative_base()


class Friending(Base):
    __tablename__ = "friending"

    id = Column(Integer, primary_key=True)
    date = Column(Date)
    action = Column(String)  # "send_request", "approve_request", "unfriend"
    action_score = Column(Integer)  # request = 1, approve = 2, unfriend = -3
    actor_id = Column(Integer, ForeignKey("user.id"))
    target_id = Column(Integer, ForeignKey("user.id"))
    pair_key = Column(String)


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)

    def __repr__(self):
        return "User(id=%s)" % self.id


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

n_user = 100
user_list = [
    User(id=id)
    for id in range(1, 1 + n_user)
]

date_list = rolex.time_series("2017-01-01", "2017-01-31", freq="1day", return_date=True)

act_request = "send_request"
act_approve = "approve_request"
act_unfriend = "unfriend"

all_action = [act_request, act_approve, act_unfriend]

friending_list = list()
friending_id = 0
for actor, target in itertools.combinations(user_list, 2):
    if random.random() <= 0.1:  # 0 ~ 0.1 sending request
        if random.random() <= 0.5:  # 0 ~ 0.5 approve
            if random.random() <= 0.1:  # 0 ~ 0.1 unfriend
                n_action = 3
            else:
                n_action = 2
        else:
            n_action = 1
    else:
        n_action = 0

    if n_action == 0:
        pass
    else:
        dates = random.sample(date_list, n_action)
        dates.sort()

        friending_id += 1
        friending_list.append(Friending(
            id=friending_id,
            date=dates[0],
            action=act_request,
            action_score=1,
            actor_id=actor.id,
            target_id=target.id,
            pair_key="%s-%s" % (actor.id, target.id),
        ))

        if n_action >= 2:
            friending_id += 1
            friending_list.append(Friending(
                id=friending_id,
                date=dates[1],
                action=act_approve,
                action_score=2,
                actor_id=target.id,
                target_id=actor.id,
                pair_key="%s-%s" % (actor.id, target.id),
            ))

            if n_action == 3:
                friending_id += 1
                friending_list.append(Friending(
                    id=friending_id,
                    date=dates[2],
                    action=act_unfriend,
                    action_score=-3,
                    actor_id=target.id,
                    target_id=actor.id,
                    pair_key="%s-%s" % (actor.id, target.id),
                ))

ses.add_all(user_list)
ses.add_all(friending_list)
ses.commit()


# preview(User, engine, limit=10)
# preview(Friending, engine, limit=10)


def q1():
    """
    计算在2017-01-15这一天里, 最终被同意的请求数的比例.
    """

    # F1.date as f1_date,
    # F1.actor_id as f1_actor_id,
    # F1.target_id as f1_target_id,
    # F1.action as f1_action,
    # F2.date as f2_date,
    # F2.actor_id as f2_actor_id,
    # F2.target_id as f2_target_id,
    # F2.action as f2_action

    sqlite_sql = sqlalchemy.text("""
        SELECT COUNT(*)
        FROM friending as F1
        JOIN friending as F2
        ON F1.actor_id = F2.target_id and F1.target_id = F2.actor_id
        WHERE
            F1.date = '2017-01-15'
            AND F1.date <= F2.date
            AND F1.action == 'send_request'
            AND F2.action == 'approve_request'
        """)
    pprint(sqlite_sql, engine)

    sqlite_sql = sqlalchemy.text("""
        SELECT COUNT(*)
        FROM friending
        WHERE
            friending.date = '2017-01-15'
            AND friending.action == 'send_request'
        """)
    pprint(sqlite_sql, engine)


q1()


def q2():
    """
    Who got the most firends? (How many friends everybody have?)
    """

    def solution1():
        """
        assign 1 to ``send_request``, 2 to ``approve_request``, -3 to ``unfriend``.
        if the total score of a pair_key is 3, means the two person on this pair are friend.

        统计出所有总分为3的pair_key, 然后分拆成两个user_id, 然后统计每个人有多少个朋友即可.
        """
        sqlite_sql = sqlalchemy.text("""
        SELECT
            F.pair_key,
            COUNT(F.action_score) as total_score
        FROM friending as F
        GROUP BY F.pair_key
        HAVING COUNT(F.action_score) = 3
        """)
        pprint(sqlite_sql, engine)

    solution1()


q2()

ses.close()
