#!/usr/bin/env python
import sys

for line in sys.stdin:
    line = line.strip().lower()
    for ch in '.,;:!?()[]{}"\'-_/\\@#$%^&*+=~`<>':
        line = line.replace(ch, ' ')
    for word in line.split():
        if word:
            print('{}\t1'.format(word))
