# -*- coding: utf-8 -*-

from learn_sql_in_python.db_sqlite_in_memory import engine

import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import String, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_mate.pt import from_everything

Base = declarative_base()


class Table1(Base):
    __tablename__ = "t1"

    id = Column(Integer, primary_key=True)
    key = Column(String)
    value = Column(Integer)


class Table2(Base):
    __tablename__ = "t2"

    id = Column(Integer, primary_key=True)
    key = Column(String)
    value = Column(Integer)


Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
ses = Session()

ses.add_all([
    Table1(key="a", value=1),
    Table1(key="b", value=2),
    Table1(key="b", value=3),
])
ses.add_all([
    Table2(key="b", value=2),
    Table2(key="b", value=3),
    Table2(key="c", value=4),
])

ses.add_all([
    Table1(key="a", value=1),
    Table1(key="b", value=2),
    Table1(key="c", value=3),
])
ses.add_all([
    Table2(key="a", value=1),
    Table2(key="b", value=2),
    Table2(key="c", value=3),
    Table2(key="a", value=1),
    Table2(key="a", value=4),
])
ses.commit()


def example():
    # --- INNER JOIN
    print(from_everything(Table1, engine))
    print("INNER JOIN")
    print(from_everything(Table2, engine))

    sql = sqlalchemy.text("""
    SELECT
        t1.id as i1,
        t1.key as k1,
        t1.value as v1,
        t2.id as i2,
        t2.key as k2,
        t2.value as v2
    FROM t1 
    INNER JOIN t2
    ON t1.key = t2.key AND t1.value = t2.value
    """)
    print("IS:")
    print(from_everything(sql, engine))

    # --- LEFT JOIN
    print(from_everything(Table1, engine))
    print("LEFT JOIN")
    print(from_everything(Table2, engine))
    sql = sqlalchemy.text("""
    SELECT
        t1.key as k1,
        t1.value as v1,
        t2.key as k2,
        t2.value as v2
    FROM t1 
    LEFT JOIN t2
    ON t1.key = t2.key 
    """)
    print("IS:")
    print(from_everything(sql, engine))


example()
