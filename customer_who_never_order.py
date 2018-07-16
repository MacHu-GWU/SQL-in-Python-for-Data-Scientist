#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
- DISTINCT function
- SELECT in WHERE clause
"""

from __future__ import print_function, division
import random
from sfm import rnd

import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import String, Integer, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import func, text
from util import pprint

engine = sqlalchemy.create_engine("sqlite:///:memory:")

Base = declarative_base()


class Customer(Base):
    __tablename__ = "customer"

    id = Column(Integer, primary_key=True)
    name = Column(String)


class Order(Base):
    __tablename__ = "shopping_order"

    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey("customer.id"))


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

n_customer = 100
n_order = 300

customer_list = list()
for id in range(1, n_customer + 1):
    customer = Customer(
        id=id,
        name=rnd.name(),
    )
    customer_list.append(customer)

order_list = list()
for id in range(1, n_order + 1):
    order = Order(
        id=id,
        customer_id=random.randint(1, n_customer + 1),
    )
    order_list.append(order)

ses.add_all(customer_list)
ses.add_all(order_list)
ses.commit()


def example1():
    """
    find all customers who never order anything.
    """
    query = ses.query(Customer.name) \
        .filter(
        Customer.id.notin_(
            ses.query(func.distinct(Order.customer_id))
        )
    )
    pprint(query, engine)


example1()
