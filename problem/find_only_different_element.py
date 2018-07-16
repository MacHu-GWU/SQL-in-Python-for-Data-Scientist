#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Given an list A of objects and another list B which is identical to A
except that one element is removed, find that removed element.
"""

import random

n = 10
lower = 1
upper = 5
l1 = [random.randint(lower, upper) for _ in range(n)]
l2 = random.sample(l1, n - 1)


def solution():
    """
    It works only if all element is integer.
    """
    return sum(l1) - sum(l2)


def find_only_different_element(l1, l2):
    d = dict()

    for i in l1:
        try:
            d[i] += 1
        except:
            d[i] = 1

    for i in l2:
        d[i] -= 1

    for i, count in d.items():
        if count != 0:
            print(i)
            return i


assert find_only_different_element(l1, l2) == solution()
