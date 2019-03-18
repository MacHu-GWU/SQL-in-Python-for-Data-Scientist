# -*- coding: utf-8 -*-

import sqlalchemy
from .local_postgres_in_container import connect_local_postgres_in_container

def reset_db(engine):
    metadata = sqlalchemy.MetaData()
    metadata.reflect(engine)
    for table in metadata.tables.values():
        table.drop(engine, checkfirst=True)
