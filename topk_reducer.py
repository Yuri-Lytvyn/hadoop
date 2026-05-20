#!/usr/bin/env python
import sys
import os
from collections import defaultdict

K = int(os.environ.get('TOPK', '20'))
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

top_k = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:K]

for rank, (word, count) in enumerate(top_k, 1):
    print('{}\t{}\t{}'.format(rank, count, word))
