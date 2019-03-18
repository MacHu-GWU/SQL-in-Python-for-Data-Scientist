#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Package Description.
"""

__version__ = "0.0.1"
__short_description__ = "Package short description."
__license__ = "MIT"
__author__ = "Sanhe Hu"
__author_email__ = "husanhe@gmail.com"
__maintainer__ = "Sanhe Hu"
__maintainer_email__ = "husanhe@gmail.com"
__github_username__ = "MacHu-GWU"

try:
    from .playground import Playground, sqlalchemy as sa
    from .db import (
        connect_local_postgres_in_container
    )
except ImportError as e:
    pass
