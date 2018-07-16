#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Given an item and timestamp, find how many of it in inventory.
"""

from __future__ import print_function, division
import rolex
import random
from datetime import datetime

import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import Integer, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from learn_sql_in_python.db import postgres_engine as engine
from learn_sql_in_python.util import pprint

Base = declarative_base()


class Inventory(Base):
    __tablename__ = "inventory"
    item_id = Column(Integer, primary_key=True)
    create_at = Column(DateTime)
    quantity = Column(Integer)


class Event(Base):
    __tablename__ = "event"

    id = Column(Integer, primary_key=True)
    create_at = Column(DateTime)
    item_id = Column(Integer)
    increment = Column(Integer)


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

n_item = 3
inventory_list = [
    Inventory(
        item_id=item_id,
        create_at=datetime(2017, 1, 8),
        quantity=100,
    )
    for item_id in range(1, 1 + n_item)
]

n_event = 30
event_list = [
    Event(
        id=id,
        create_at=rolex.rnd_datetime("2017-01-01", "2017-01-14 23:59:59"),
        item_id=random.randint(1, 1 + n_item),
        increment=random.randint(1, 3),
    )
    for id in range(1, 1 + n_event)
]

ses.add_all(inventory_list)
ses.add_all(event_list)

ses.commit()

# preview(Inventory, engine, limit=10)
# preview(Event, engine, limit=10)


def solution1():
    """
    If the ``item_id`` is given.
    """
    query = sqlalchemy.text("""
    SELECT
        event.item_id,
        event.create_at,
        event.increment,
        sum(event.increment) OVER (ORDER BY event.create_at) as cum_sum,
        inventory.create_at as ref_time,
        inventory.quantity as ref_quantity,
        (
            SELECT SUM(event.increment)
            FROM event
            WHERE event.item_id = 1
                AND event.create_at < inventory.create_at
        ) as total_inc_before_ref_time,
        sum(event.increment) OVER (ORDER BY event.create_at) + inventory.quantity - (
            SELECT SUM(event.increment)
            FROM event
            WHERE event.item_id = 1
                AND event.create_at < inventory.create_at
        ) - event.increment as real_time_quantity
    FROM event 
    JOIN inventory
    ON event.item_id = inventory.item_id
    WHERE event.item_id = 1
    """)
    pprint(query, engine)


# solution1()


def solution2():
    # 计算每个物品的cum_sum
    query = sqlalchemy.text("""
    SELECT 
        event.item_id,
        event.create_at,
        event.increment,
        sum(event.increment) OVER (
            PARTITION BY event.item_id 
            ORDER BY event.item_id, event.create_at
        ) as cum_sum,
        inventory.create_at as ref_time,
        inventory.quantity as ref_quantity
    FROM event 
    JOIN inventory
    ON event.item_id = inventory.item_id
    """)
    pprint(query, engine)

    # 计算每个物品的最初值
    query = sqlalchemy.text("""
    SELECT 
        event.item_id as item_id,
        AVG(inventory.quantity) - SUM(event.increment) as start_quantity
    FROM event 
    JOIN inventory
    ON event.item_id = inventory.item_id
    WHERE event.create_at < inventory.create_at
    GROUP BY event.item_id
    ORDER BY event.item_id
    """)
    pprint(query, engine)

    # 用cum_sum + start_quantity就是最后的总数
    query = sqlalchemy.text("""
    SELECT 
        event.item_id,
        event.create_at,
        event.increment,
        sum(event.increment) OVER (
            PARTITION BY event.item_id 
            ORDER BY event.item_id, event.create_at
        ) as cum_sum,
        CAST(
            (
                sum(event.increment) OVER (
                    PARTITION BY event.item_id 
                    ORDER BY event.item_id, event.create_at
                ) + start.start_quantity
            ) AS INTEGER
        ) as realtime_quantity
    FROM event
    JOIN (
        SELECT 
            event.item_id as item_id,
            AVG(inventory.quantity) - SUM(event.increment) as start_quantity
        FROM event 
        JOIN inventory
        ON event.item_id = inventory.item_id
        WHERE event.create_at < inventory.create_at
        GROUP BY event.item_id
        ORDER BY event.item_id
    ) as start
    ON
    event.item_id = start.item_id
    """)
    pprint(query, engine)


solution2()

ses.close()
