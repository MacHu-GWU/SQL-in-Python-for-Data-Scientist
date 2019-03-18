#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlalchemy


def clean_all(engine):
    """
    Remove all data.
    """
    metadata = sqlalchemy.MetaData()
    metadata.reflect(engine)

    for _ in range(3):
        for table in metadata.sorted_tables:
            try:
                table.drop(engine)
            except:
                pass

postgres_engine = sqlalchemy.create_engine(
    "postgresql+psycopg2://asoidnsj:m9PLClSSHt1wVmxL_qlbZpNh5lDFWQRe@stampy.db.elephantsql.com:5432/asoidnsj"
)
clean_all(postgres_engine)

sqlite_in_memory_engine = sqlalchemy.create_engine("sqlite:///:memory:")

