#!/usr/bin/python

from tokenize import Double
import sys
import socket
import string
import re
import fcntl
import getpass
import logging
import platform


def SUM(C, N):
    assert isinstance(N, int), \
        'type {} does not match original type {}'.format(type(N), int)
                                                         
    
    assert isinstance(C, float), \
        'type {} does not match original type {}'.format(type(N), float)
                    
    sum1 = 0.0
    i = 0
    for i in range(1, N-1):
        sum1 = sum1 + REF(C, i)
    sum1 = sum1 + C
    return sum1


def SLOPE(X, N):
    result = 0
    square = 0
    i = 0
    for i in range(1, N-1):
        result = result + i*REF(X, N-i)
    result = result + N * X

    for i in range(1, N):
        square = square + i*i  
    result = result - SUM(X, N)* ((1+N)/2) / square - ((1+N)/2)*((1+N)/2)

    return result

