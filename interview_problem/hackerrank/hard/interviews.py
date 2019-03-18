# -*- coding: utf-8 -*-

"""


- contest: 比赛, 一个比赛里面可能有多个题目.
- challenge: 题目, 一个题目只会出现在一个比赛中.
- college: 学校, 一个比赛可能在多个学校举行, 但是一个学校只能举行一个比赛.
- hacker: 学生
"""
from learn_sql_in_python import sa, connect_local_postgres_in_container, Playground

engine = connect_local_postgres_in_container()
pg = Playground(engine)

metadata = sa.MetaData()
t_contests = sa.Table(
    "contests", metadata,
    sa.Column("contest_id", sa.Integer),
    sa.Column("hacker_id", sa.Integer),
    sa.Column("hacker_name", sa.String),
)

t_colleges = sa.Table(
    "colleges", metadata,
    sa.Column("college_id", sa.Integer),
    sa.Column("contest_id", sa.Integer),
)

t_challenges = sa.Table(
    "challenges", metadata,
    sa.Column("challenge_id", sa.Integer),
    sa.Column("college_id", sa.Integer),
)

t_view_stats = sa.Table(
    "view_stats", metadata,
    sa.Column("challenge_id", sa.Integer),
    sa.Column("total_views", sa.Integer),
    sa.Column("total_unique_views", sa.Integer),
)

t_submission_stats = sa.Table(
    "submission_stats", metadata,
    sa.Column("challenge_id", sa.Integer),
    sa.Column("total_submissions", sa.Integer),
    sa.Column("total_accepted_submissions", sa.Integer),
)

pg.add_table(
    metadata, t_contests,
    [
        dict(contest_id=66406, hacker_id=17973, name="Rose"),
        dict(contest_id=66556, hacker_id=79153, name="Angela"),
        dict(contest_id=94828, hacker_id=80275, name="Frank"),
    ]
)

pg.add_table(
    metadata, t_colleges,
    [
        dict(college_id=11219, contest_id=66406),
        dict(college_id=32473, contest_id=66556),
        dict(college_id=56685, contest_id=94828),
    ]
)

pg.add_table(
    metadata, t_challenges,
    [
        dict(challenge_id=18765, college_id=11219),
        dict(challenge_id=47127, college_id=11219),
        dict(challenge_id=60292, college_id=32473),
        dict(challenge_id=72974, college_id=56685),
    ]
)


pg.add_table(
    metadata, t_view_stats,
    [
        dict(challenge_id=47127, total_views=26, total_unique_views=19),
        dict(challenge_id=47127, total_views=15, total_unique_views=14),
        dict(challenge_id=18765, total_views=43, total_unique_views=10),
        dict(challenge_id=18765, total_views=72, total_unique_views=13),
        dict(challenge_id=75516, total_views=35, total_unique_views=17),
        dict(challenge_id=60292, total_views=11, total_unique_views=10),
        dict(challenge_id=72974, total_views=41, total_unique_views=15),
        dict(challenge_id=75516, total_views=75, total_unique_views=11),
    ]
)
pg.add_table(
    metadata, t_submission_stats,
    [
        dict(challenge_id=75516, total_submissions=34, total_accepted_submissions=12),
        dict(challenge_id=47127, total_submissions=27, total_accepted_submissions=10),
        dict(challenge_id=47127, total_submissions=56, total_accepted_submissions=18),
        dict(challenge_id=75516, total_submissions=74, total_accepted_submissions=12),
        dict(challenge_id=75516, total_submissions=83, total_accepted_submissions=8),
        dict(challenge_id=72974, total_submissions=68, total_accepted_submissions=24),
        dict(challenge_id=72974, total_submissions=82, total_accepted_submissions=14),
        dict(challenge_id=47127, total_submissions=28, total_accepted_submissions=11),
    ]
)
