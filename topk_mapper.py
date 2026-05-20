#!/usr/bin/env python
import sys
from collections import defaultdict

word_counts = defaultdict(int)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        parts = line.split('\t', 1)
        count = int(parts[0])
        word = parts[1]
    except (ValueError, IndexError):
        continue
    word_counts[word] += count

for word, count in word_counts.items():
    print('{}\t{}'.format(count, word))
