# -*- coding: utf-8 -*-

"""
LEAD, LAG, USAGE
"""

import rolex
from learn_sql_in_python import (
    sa, connect_local_postgres_in_container, Playground
)

engine = connect_local_postgres_in_container()
pg = Playground(engine)

metadata = sa.MetaData()
t_weather = sa.Table(
    "weather", metadata,
    sa.Column("date", sa.Date),
    sa.Column("temp", sa.Integer),
)
t_weather_data = [
    dict(
        date=a_date,
        temp=10 + ind,
    )
    for ind, a_date in enumerate(
        rolex.time_series("2017-01-01", "2017-01-07", freq="1day", return_date=True)
    )
]
pg.add_table(metadata, t_weather, t_weather_data)


def lead_lag():
    sql = """
    SELECT 
        weather.date,
        weather.temp,
        LEAD(weather.temp, 1) OVER (ORDER BY weather.date) as next1,
        LEAD(weather.temp, 2, 0) OVER (ORDER BY weather.date) as next2,
        LAG(weather.temp, 1) OVER (ORDER BY weather.date) as previous1,
        LAG(weather.temp, 2, 0) OVER (ORDER BY weather.date) as previous2,
        weather.temp - LAG(weather.temp, 1) OVER (ORDER BY weather.date) as increment
    FROM weather
    """
    pg.print_result(sql)


lead_lag()


def derivative():
    sql = """
    SELECT 
        weather.date as start,
        LEAD(weather.date, 1) OVER (ORDER BY weather.date) as end,
        weather.temp as start_temp,
        LEAD(weather.temp, 1) OVER (ORDER BY weather.date) as end_temp,
        LEAD(weather.temp, 1) OVER (ORDER BY weather.date) - weather.temp as increament
    FROM weather
    """
    pg.print_result(sql)


derivative()
