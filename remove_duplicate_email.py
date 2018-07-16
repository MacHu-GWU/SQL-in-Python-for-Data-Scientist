#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Handling Duplicate
"""

from __future__ import print_function, division
import random
from sfm import rnd

import sqlalchemy
from sqlalchemy import Table, Column
from sqlalchemy import String, Integer, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base

from learn_sql_in_python.db import engine
from learn_sql_in_python.util import pprint, preview


Base = declarative_base()

class Email(Base):
    __tablename__ = "email"

    id = Column(Integer, primary_key=True)
    email = Column(String)


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

data = [
    Email(id=1, email="alice@example.com"),
    Email(id=2, email="bob@example.com"),
    Email(id=3, email="cathy@example.com"),
    Email(id=4, email="alice@example.com"),
    Email(id=5, email="alice@example.com"),
    Email(id=6, email="bob@example.com"),
    Email(id=7, email="bob@example.com"),
]

ses.add_all(data)
ses.commit()


preview(Email, engine)

def find_unique_email():
    query = sqlalchemy.text("""
    SELECT DISTINCT(email.email)
    FROM email
    """)
    pprint(query, engine)

# find_unique_email()

def find_duplicate_email():
    query = sqlalchemy.text("""
    SELECT email.id, email.email
    FROM email
    GROUP BY email.email
    HAVING COUNT(*) >= 2
    """)
    pprint(query, engine)

# find_duplicate_email()

def remove_duplicate_email():
    query = sqlalchemy.text("""
    DELETE e1
    FROM email as e1, email as e2
    WHERE
        e1.email = e2.email
        AND e1.id > e2.id
    """)
    # query = sqlalchemy.text("""
    # DELETE *
    # """)
    pprint(query, engine)

    preview(Email, engine)

remove_duplicate_email()
# preview(Email, engine)