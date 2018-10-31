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
ses.commit()


def example_inner_join():
    """
    ================================================================================
    t1
    +----+-----+-------+
    | id | key | value |
    +----+-----+-------+
    | 1  |  a  |   1   |
    | 2  |  b  |   2   |
    | 3  |  b  |   3   |
    +----+-----+-------+
    INNER JOIN
    t2
    +----+-----+-------+
    | id | key | value |
    +----+-----+-------+
    | 1  |  b  |   2   |
    | 2  |  b  |   3   |
    | 3  |  c  |   4   |
    +----+-----+-------+
    ON t1.id = t2.id
    IS:
    +----+----+----+----+----+----+
    | i1 | k1 | v1 | i2 | k2 | v2 |
    +----+----+----+----+----+----+
    | 1  | a  | 1  | 1  | b  | 2  |
    | 2  | b  | 2  | 2  | b  | 3  |
    | 3  | b  | 3  | 3  | c  | 4  |
    +----+----+----+----+----+----+
    ================================================================================
    t1
    +----+-----+-------+
    | id | key | value |
    +----+-----+-------+
    | 1  |  a  |   1   |
    | 2  |  b  |   2   |
    | 3  |  b  |   3   |
    +----+-----+-------+
    INNER JOIN
    t2
    +----+-----+-------+
    | id | key | value |
    +----+-----+-------+
    | 1  |  b  |   2   |
    | 2  |  b  |   3   |
    | 3  |  c  |   4   |
    +----+-----+-------+
    ON t1.key = t2.key
    IS:
    +----+----+----+----+----+----+
    | i1 | k1 | v1 | i2 | k2 | v2 |
    +----+----+----+----+----+----+
    | 2  | b  | 2  | 1  | b  | 2  |
    | 2  | b  | 2  | 2  | b  | 3  |
    | 3  | b  | 3  | 1  | b  | 2  |
    | 3  | b  | 3  | 2  | b  | 3  |
    +----+----+----+----+----+----+
    ================================================================================
    t1
    +----+-----+-------+
    | id | key | value |
    +----+-----+-------+
    | 1  |  a  |   1   |
    | 2  |  b  |   2   |
    | 3  |  b  |   3   |
    +----+-----+-------+
    INNER JOIN
    t2
    +----+-----+-------+
    | id | key | value |
    +----+-----+-------+
    | 1  |  b  |   2   |
    | 2  |  b  |   3   |
    | 3  |  c  |   4   |
    +----+-----+-------+
    ON t1.value = t2.value
    IS:
    +----+----+----+----+----+----+
    | i1 | k1 | v1 | i2 | k2 | v2 |
    +----+----+----+----+----+----+
    | 2  | b  | 2  | 1  | b  | 2  |
    | 3  | b  | 3  | 2  | b  | 3  |
    +----+----+----+----+----+----+
    """
    # ON id
    print("=" * 80)
    print("t1")
    print(from_everything(Table1, engine))
    print("INNER JOIN")
    print("t2")
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
    ON t1.id = t2.id
    """)
    print("ON t1.id = t2.id")
    print("IS:")
    print(from_everything(sql, engine))

    # ON key
    print("=" * 80)
    print("t1")
    print(from_everything(Table1, engine))
    print("INNER JOIN")
    print("t2")
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
    ON t1.key = t2.key
    """)
    print("ON t1.key = t2.key")
    print("IS:")
    print(from_everything(sql, engine))

    # ON value
    print("=" * 80)
    print("t1")
    print(from_everything(Table1, engine))
    print("INNER JOIN")
    print("t2")
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
    ON t1.value = t2.value
    """)
    print("ON t1.value = t2.value")
    print("IS:")
    print(from_everything(sql, engine))


def example_left_join():
    print("t1")
    print(from_everything(Table1, engine))
    print("LEFT JOIN")
    print("t2")
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
    LEFT JOIN t2
    ON t1.key = t2.key
    """)
    print("ON t1.key = t2.key")
    print("IS:")
    print(from_everything(sql, engine))


if __name__ == "__main__":
    example_inner_join()
    # example_left_join()
