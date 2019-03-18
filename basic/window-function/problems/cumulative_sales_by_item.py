# -*- coding: utf-8 -*-

"""
**Question**:

- :func:`apple_sales_by_month`: 统计苹果每个月的总销售额
- :func:`apple_cum_sales_by_month`: 统计苹果每个月的累计销售额
- :func:`all_items_cum_sales_by_month`: 跟上个一样, 不过统计所有的物品品种
- :func:`apple_monthly_cum_sales_by_days`: 统计苹果在每个月内, 每天的累计销售额. 每到下个月初, 累计销售额清0重新计算.
- :func:`all_items_monthly_cum_sales_by_days`: 跟上一个一样, 不过统计所有的物品品种
"""

import rolex
import random
from learn_sql_in_python import sa, connect_local_postgres_in_container, Playground

engine = connect_local_postgres_in_container()
pg = Playground(engine)

metadata = sa.MetaData()
t_sales = sa.Table(
    "sales", metadata,
    sa.Column("item", sa.String),
    sa.Column("sales", sa.Float),
    sa.Column("time", sa.DateTime),
)

item_list = ["Apple", "Banana"]
n_rows = 1000
t_sales_data = [
    dict(
        item=random.choice(item_list),
        sales=float("%.2f" % (random.random() * 3 + 1,)),
        time=rolex.rnd_datetime("2019-01-01", "2020-01-01"))
    for _ in range(n_rows)
]
pg.add_table(metadata, t_sales, t_sales_data)


# ---
def apple_sales_by_month():
    sql = """
    SELECT
        date_trunc('month', sales.time) AS month,
        SUM(sales.sales) as sales
    FROM sales
    WHERE sales.item = 'Apple' 
    GROUP BY month
    ORDER BY month
    """
    pg.print_result(sql)


# apple_sales_by_month()


def apple_cum_sales_by_month():
    sql = """
    WITH 
        t_apple_monthly_sales AS (
            SELECT
                date_trunc('month', sales.time) AS month,
                SUM(sales.sales) as sales
            FROM sales
            WHERE sales.item = 'Apple' 
            GROUP BY month
            ORDER BY month
        )
    SELECT
        t_apple_monthly_sales.month AS month,
        SUM(t_apple_monthly_sales.sales) OVER (
            ORDER BY t_apple_monthly_sales.month
        )
    FROM t_apple_monthly_sales
    """
    pg.print_result(sql)


# apple_cum_sales_by_month()


def all_items_cum_sales_by_month():
    sql = """
    WITH 
        t_monthly_sales AS (
            SELECT
                sales.item AS item,
                date_trunc('month', sales.time) AS month,
                SUM(sales.sales) as sales
            FROM sales 
            GROUP BY item, month
            ORDER BY item, month
        )
    SELECT
        t_monthly_sales.item AS item,
        t_monthly_sales.month AS month,
        SUM(t_monthly_sales.sales) OVER (
            PARTITION BY t_monthly_sales.item
            ORDER BY t_monthly_sales.month
        )
    FROM t_monthly_sales
    """
    pg.print_result(sql)


# all_items_cum_sales_by_month()


def apple_monthly_cum_sales_by_days():
    sql = """
    WITH 
        t_apple_daily_sales AS (
            SELECT
                date_trunc('day', sales.time) AS day,
                SUM(sales.sales) as sales
            FROM sales
            WHERE sales.item = 'Apple' 
            GROUP BY day
            ORDER BY day
        )
    SELECT
        t_apple_daily_sales.day AS day,
        SUM(t_apple_daily_sales.sales) OVER (
            PARTITION BY date_trunc('month', t_apple_daily_sales.day) 
            ORDER BY t_apple_daily_sales.day
        )
    FROM t_apple_daily_sales
    """
    pg.print_result(sql)


# apple_monthly_cum_sales_by_days()


def all_items_monthly_cum_sales_by_days():
    sql = """
    WITH 
        t_daily_sales AS (
            SELECT
                sales.item AS item,
                date_trunc('day', sales.time) AS day,
                SUM(sales.sales) as sales
            FROM sales
            GROUP BY item, day
            ORDER BY item ASC, day ASC
        )
    SELECT
        t_daily_sales.item AS item,
        t_daily_sales.day AS day,
        SUM(t_daily_sales.sales) OVER (
            PARTITION BY t_daily_sales.item, date_trunc('month', t_daily_sales.day) 
            ORDER BY t_daily_sales.day
        )
    FROM t_daily_sales
    """
    pg.print_result(sql)

# all_items_monthly_cum_sales_by_days()
