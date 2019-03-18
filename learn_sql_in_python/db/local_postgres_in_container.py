# -*- coding: utf-8 -*-

"""
run postgres in local container, image used: https://hub.docker.com/_/postgres
"""

from sqlalchemy_mate import EngineCreator, test_connection


def connect_local_postgres_in_container():
    credential = dict(
        host="localhost",
        port=5432,
        database="postgres",
        username="postgres",
        password="password",
    )
    engine = EngineCreator(**credential).create_postgresql_psycopg2()
    # test_connection(engine)
    return engine
