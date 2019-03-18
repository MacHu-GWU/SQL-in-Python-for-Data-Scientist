# -*- coding: utf-8 -*-

"""

"""

from datetime import datetime
from learn_sql_in_python import sa, connect_local_postgres_in_container, Playground

engine = connect_local_postgres_in_container()
pg = Playground(engine)

metadata = sa.MetaData()
t_employees = sa.Table(
    "employees", metadata,
    sa.Column("id", sa.Integer),
    sa.Column("hire_date", sa.Integer),
    sa.Column("level", sa.Integer),
)

t_promotions = sa.Table(
    "promotions", metadata,
    sa.Column("employee_id", sa.Integer),
    sa.Column("promotion_date", sa.Integer),
    sa.Column("level", sa.Integer),
)

t_employees_data = [
    dict(id=1, hire_date=2001, level=1),
    dict(id=2, hire_date=2002, level=1),
    dict(id=3, hire_date=2003, level=1),
    dict(id=4, hire_date=2004, level=1),
    dict(id=5, hire_date=2005, level=1),
]

t_promotions_data = [
    dict(employee_id=1, promotion_date=2003, level=2),
    dict(employee_id=1, promotion_date=2008, level=3),
    dict(employee_id=1, promotion_date=2010, level=4),
    dict(employee_id=2, promotion_date=2005, level=2),
    dict(employee_id=2, promotion_date=2007, level=3),
    dict(employee_id=2, promotion_date=2011, level=4),
    dict(employee_id=2, promotion_date=2018, level=5),
    dict(employee_id=3, promotion_date=2008, level=2),
    dict(employee_id=3, promotion_date=2014, level=3),
    dict(employee_id=4, promotion_date=2007, level=2),
    dict(employee_id=4, promotion_date=2010, level=3),
    dict(employee_id=4, promotion_date=2013, level=4),
    dict(employee_id=5, promotion_date=2006, level=2),
]

pg.add_table(metadata, t_employees, t_employees_data)
pg.add_table(metadata, t_promotions, t_promotions_data)


def example():
    sql = """
    SELECT
        T.previous_level AS from_level,
        T.current_level AS to_level,
        AVG(T.promotion_date - T.previous_date) AS duration,
        COUNT(*) AS n_sample
    FROM
    (
        SELECT 
            P.employee_id,
            E.hire_date,
            E.level AS init_level,
            COALESCE(LAG(P.promotion_date, 1) OVER (PARTITION BY P.employee_id ORDER BY P.level), E.hire_date) AS previous_date,
            P.promotion_date,
            COALESCE(LAG(P.level, 1) OVER (PARTITION BY P.employee_id ORDER BY P.level), E.level) AS previous_level,
            P.level AS current_level
        FROM 
            promotions P
        JOIN
            employees E
        ON
            P.employee_id = E.id
    ) T
    GROUP BY from_level, to_level
    ORDER BY from_level ASC, to_level ASC
    """
    pg.print_result(sql)


example()
