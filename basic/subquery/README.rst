将一个 Query 的结果视为一个临时表, 然后再后续的计算中引用这个临时表, 能有助于简化你的 SQL 代码, 将抽象概念分离, 优化查询.

我们有一个超市销售数据, 记录了各个品类的物品, 在什么时候, 卖出了多少钱. 那么假设我们要分析在 2018 年里, 各个品类的物品在每个月的销售额. 我们有如下 SQL:

.. code-block:: SQL

    SELECT
        orders.item AS item,
        date_trunc('month', orders.time) AS month,
        SUM(orders.sales) AS total_sales
    FROM orders
    WHERE
        orders.time BETWEEN '2018-01-01' AND '2019-01-01'
    GROUP BY item, month
    ORDER BY item, month

如果一个 Query 的结果被引用多次, 从性能的角度考虑, 我们当然希望这个 Query 只被执行一次.

WITH 关键字就是做这个的. WITH 相当于定义了一个只对该条 SQL 生效的临时表. 那么以上的代码可以被改写为:

.. code-block:: SQL

    WITH
        t_y2018 AS (
            SELECT
                orders.item AS item,
                orders.time AS time,
                orders.sales AS sales
            FROM orders
            WHERE
                orders.time BETWEEN '2018-01-01' AND '2019-01-01'
        )
    -- put complex query for 2018 data here
    SELECT
        t_y2018.item AS item,
        date_trunc('month', t_y2018.time) AS month,
        SUM(t_y2018.sales) AS total_sales
    FROM t_y2018
    GROUP BY item, month
    ORDER BY item, month

WITH 语句还能按顺序依次构建多个临时表, 后构建的表可以引用先构建的表.

.. code-block:: SQL

    WITH
        t1 AS (
            SELECT ...
        ),
        t2 AS (
            SELCT ... FROM t1 ...
        )

WITH 和 临时表 (TEMP TABLE) 的区别:

- WITH 所定义的表只存在于一条 SQL 语句以内, SQL 语句结束时数据会自动清除.
- TEMP TABLE 会在一个 Session, 数据库连接, Transaction 结束时自动 Drop.

Reference:

- WITH Clause: https://www.postgresql.org/docs/current/queries-with.html
