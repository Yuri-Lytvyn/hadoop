#!/usr/bin/env python
import sys

current_word = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    try:
        word, count = line.split('\t', 1)
        count = int(count)
    except ValueError:
        continue
    if current_word == word:
        current_count += count
    else:
        if current_word:
            print('{}\t{}'.format(current_count, current_word))
        current_count = count
        current_word = word

if current_word:
    print('{}\t{}'.format(current_count, current_word))
