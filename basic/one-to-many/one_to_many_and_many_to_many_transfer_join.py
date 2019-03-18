# -*- coding: utf-8 -*-

"""
假设数据库中的 3 个 Entity 分别是 A-B = one-to-many, B-C = one-to-many.
那么一个 A 就会间接地对应很多 C. 这种情况下, 我们通常会用到 2 个 JOIN 来连接数据.

1个部门有多个雇员, 1个雇员只能从属于一个部门
1个总经理管理多个部门, 1个部门只有一个总经理
1个项目有许多个部门参与, 1个部门也参与了多个项目
"""

import random
from learn_sql_in_python import sa, connect_local_postgres_in_container, Playground

engine = connect_local_postgres_in_container()
pg = Playground(engine)
metadata = sa.MetaData()

t_employees = sa.Table(
    "employees", metadata,
    sa.Column("id", sa.Integer),
    sa.Column("department_id", sa.Integer),
)

t_departments = sa.Table(
    "departments", metadata,
    sa.Column("id", sa.Integer),
    sa.Column("vp_id", sa.Integer),
)

t_vps = sa.Table(
    "vps", metadata,
    sa.Column("id", sa.Integer),
)

t_project = sa.Table(
    "projects", metadata,
    sa.Column("id", sa.Integer),
)

t_project_and_department = sa.Table(
    "project_and_department", metadata,
    sa.Column("project_id", sa.Integer),
    sa.Column("department_id", sa.Integer),
)

n_vps = 5
t_vps_data = [dict(id=id) for id in range(1, n_vps + 1)]

n_departments = 25
t_departments_data = [dict(id=id, vp_id=random.randint(1, n_vps)) for id in range(1, n_departments + 1)]

n_employees = 500
t_employees_data = [dict(id=id, department_id=random.randint(1, n_departments)) for id in range(1, n_employees + 1)]

n_projects = 40
t_projects_data = [dict(id=id) for id in range(1, n_projects + 1)]

t_project_and_department_data = list()
department_id_list = list(range(1, n_departments + 1))
for project_id in range(1, n_projects + 1):
    for department_id in random.sample(department_id_list, random.randint(3, 5)):
        t_project_and_department_data.append(dict(project_id=project_id, department_id=department_id))

pg.add_table(metadata, t_vps, t_vps_data)
pg.add_table(metadata, t_departments, t_departments_data)
pg.add_table(metadata, t_employees, t_employees_data)
pg.add_table(metadata, t_project, t_projects_data)
pg.add_table(metadata, t_project_and_department, t_project_and_department_data)


def count_n_employee_by_vice_president():
    """
    计算每个 VP 下面领导多少人
    """
    sql = """
    SELECT
        D.vp_id AS vp_id,
        COUNT(E.id) AS n_employee
    FROM 
        employees E
    JOIN
        departments D
    ON E.department_id = D.id
    GROUP BY vp_id
    ORDER BY n_employee
    """
    pg.print_result(sql)


count_n_employee_by_vice_president()


def count_n_employee_by_project():
    """
    计算每个项目有多少人参与了
    """
    sql = """
    SELECT
        P.project_id AS project_id,
        COUNT(E.id) AS n_employee
    FROM 
        employees E
    JOIN
        departments D
    ON 
        E.department_id = D.id
    JOIN
        project_and_department P
    ON 
        E.department_id = P.department_id 
    GROUP BY project_id
    ORDER BY project_id
    """
    pg.print_result(sql)


count_n_employee_by_project()
