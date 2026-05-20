#!/usr/bin/env python3
import sys
import os
import time
import random
import subprocess

VOCABULARY = {
    "the": 100, "a": 80, "an": 40, "of": 90, "in": 85,
    "to": 80, "and": 75, "is": 70, "it": 65, "for": 60,
    "on": 55, "with": 50, "as": 45, "at": 40, "by": 35,
    "from": 30, "or": 28, "was": 35, "are": 33, "be": 30,
    "this": 28, "that": 45, "not": 25, "but": 22, "had": 20,
    "have": 18, "has": 16, "will": 15, "can": 14, "all": 13,
    "if": 12, "its": 11, "more": 10, "also": 9, "new": 12,
    "data": 15, "system": 14, "time": 13, "each": 8, "which": 10,
    "about": 7, "up": 8, "out": 7, "when": 9, "would": 8,
    "parallel": 6, "computing": 5, "hadoop": 8, "mapreduce": 7,
    "cluster": 5, "distributed": 4, "processing": 5, "node": 6,
    "mapper": 4, "reducer": 4, "framework": 3, "big": 5,
    "analysis": 4, "algorithm": 3, "performance": 4, "network": 3,
    "server": 4, "file": 5, "input": 4, "output": 4,
    "task": 5, "worker": 3, "memory": 4, "storage": 3,
    "optimization": 2, "scalability": 1, "throughput": 2,
    "latency": 1, "bandwidth": 1, "replication": 1,
}


def generate_data(num_words, output_file):
    words = list(VOCABULARY.keys())
    weights = list(VOCABULARY.values())
    with open(output_file, 'w', encoding='utf-8') as f:
        line_buf = []
        for _ in range(num_words):
            word = random.choices(words, weights=weights, k=1)[0]
            line_buf.append(word)
            if len(line_buf) >= random.randint(5, 15):
                f.write(' '.join(line_buf) + '\n')
                line_buf = []
        if line_buf:
            f.write(' '.join(line_buf) + '\n')
    size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print('Generated {} words -> {} ({:.2f} MB)'.format(num_words, output_file, size_mb))


def run_sequential(input_file, k):
    word_counts = {}
    t0 = time.time()
    with open(input_file, encoding='utf-8') as f:
        for line in f:
            line = line.strip().lower()
            for ch in '.,;:!?()[]{}"\'-_/\\@#$%^&*+=~`<>':
                line = line.replace(ch, ' ')
            for word in line.split():
                if word:
                    word_counts[word] = word_counts.get(word, 0) + 1
    elapsed = time.time() - t0
    top_k = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:k]
    total = sum(word_counts.values())
    unique = len(word_counts)
    return top_k, total, unique, elapsed


def run_hadoop(input_file, k):
    jar = '/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar'
    abs_input = os.path.abspath(input_file)

    subprocess.run(
        ['docker', 'cp', abs_input, 'hadoop-lab:/opt/hadoop/input_main.txt'],
        capture_output=True
    )
    subprocess.run(
        ['docker', 'cp', 'mapper.py', 'hadoop-lab:/opt/hadoop/mapper.py'],
        capture_output=True
    )
    subprocess.run(
        ['docker', 'cp', 'reducer.py', 'hadoop-lab:/opt/hadoop/reducer.py'],
        capture_output=True
    )
    subprocess.run(
        ['docker', 'cp', 'topk_mapper.py', 'hadoop-lab:/opt/hadoop/topk_mapper.py'],
        capture_output=True
    )
    subprocess.run(
        ['docker', 'cp', 'topk_reducer.py', 'hadoop-lab:/opt/hadoop/topk_reducer.py'],
        capture_output=True
    )

    stage1 = (
        'rm -rf /tmp/wc_out /tmp/topk_out && '
        'hadoop jar {jar} '
        '-files /opt/hadoop/mapper.py,/opt/hadoop/reducer.py '
        '-mapper "python mapper.py" '
        '-reducer "python reducer.py" '
        '-input /opt/hadoop/input_main.txt '
        '-output /tmp/wc_out'
    ).format(jar=jar)

    stage2 = (
        'TOPK={k} hadoop jar {jar} '
        '-files /opt/hadoop/topk_mapper.py,/opt/hadoop/topk_reducer.py '
        '-mapper "python topk_mapper.py" '
        '-reducer "python topk_reducer.py" '
        '-input /tmp/wc_out '
        '-output /tmp/topk_out'
    ).format(jar=jar, k=k)

    t0 = time.time()
    r1 = subprocess.run(
        ['docker', 'exec', 'hadoop-lab', 'bash', '-c', stage1],
        capture_output=True, text=True
    )
    if r1.returncode != 0:
        return None, time.time() - t0

    r2 = subprocess.run(
        ['docker', 'exec', 'hadoop-lab', 'bash', '-c', stage2],
        capture_output=True, text=True
    )
    elapsed = time.time() - t0

    if r2.returncode != 0:
        return None, elapsed

    out = subprocess.run(
        ['docker', 'exec', 'hadoop-lab', 'bash', '-c',
         'cat /tmp/topk_out/part-00000'],
        capture_output=True, text=True
    )

    top_k = []
    for line in out.stdout.strip().splitlines():
        parts = line.split('\t')
        if len(parts) == 3:
            top_k.append((parts[2], int(parts[1])))

    return top_k, elapsed


def save_results(output_file, input_file, k, seq_top, seq_total, seq_unique, seq_time, hadoop_top, hadoop_time):
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('Word Count + Top {} Results\n'.format(k))
        f.write('Input: {}\n\n'.format(input_file))

        f.write('[1] Sequential\n')
        f.write('Total words : {}\n'.format(seq_total))
        f.write('Unique words: {}\n'.format(seq_unique))
        f.write('Time        : {:.4f} s\n'.format(seq_time))
        f.write('Top {}:\n'.format(k))
        for rank, (word, count) in enumerate(seq_top, 1):
            f.write('  {:3d}. {:<20s} {:>8,}\n'.format(rank, word, count))

        f.write('\n[2] Hadoop MapReduce\n')
        if hadoop_top:
            f.write('Time        : {:.4f} s\n'.format(hadoop_time))
            f.write('Top {}:\n'.format(k))
            for rank, (word, count) in enumerate(hadoop_top, 1):
                f.write('  {:3d}. {:<20s} {:>8,}\n'.format(rank, word, count))
        else:
            f.write('Hadoop not available\n')

        f.write('\n[Comparison]\n')
        f.write('Sequential : {:.4f} s\n'.format(seq_time))
        if hadoop_top:
            f.write('Hadoop     : {:.4f} s\n'.format(hadoop_time))
            ratio = hadoop_time / seq_time if seq_time else 0
            if ratio > 1:
                f.write('Hadoop is {:.1f}x slower (JVM overhead, single node)\n'.format(ratio))
            else:
                f.write('Hadoop is {:.1f}x faster\n'.format(1 / ratio))


def print_results(input_file, k, seq_top, seq_total, seq_unique, seq_time, hadoop_top, hadoop_time):
    W = 62
    size_mb = os.path.getsize(input_file) / (1024 * 1024)

    print('=' * W)
    print('  Рис. 1  Результати послідовного та MapReduce підрахунку')
    print('          слів і пошуку Top {}'.format(k))
    print('=' * W)
    print('  Вхідний файл : {}  ({:.2f} MB)'.format(input_file, size_mb))
    print()

    print('┌─ [1] Sequential Word Count ' + '─' * (W - 29) + '┐')
    print('│  Всього слів   : {:,}'.format(seq_total))
    print('│  Унікальних    : {:,}'.format(seq_unique))
    print('│  Час           : {:.4f} с'.format(seq_time))
    print('│  Top {}:'.format(k))
    for rank, (word, count) in enumerate(seq_top, 1):
        print('│    {:3d}.  {:<18s}  {:>7,}'.format(rank, word, count))
    print('└' + '─' * (W - 2) + '┘')
    print()

    print('┌─ [2] Hadoop MapReduce (Streaming) ' + '─' * (W - 36) + '┐')
    if hadoop_top:
        print('│  Час           : {:.4f} с'.format(hadoop_time))
        print('│  Top {}:'.format(k))
        for rank, (word, count) in enumerate(hadoop_top, 1):
            print('│    {:3d}.  {:<18s}  {:>7,}'.format(rank, word, count))
    else:
        print('│  [!] Hadoop недоступний. Запустіть: docker start hadoop-lab')
    print('└' + '─' * (W - 2) + '┘')
    print()

    print('┌─ Порівняння ' + '─' * (W - 14) + '┐')
    print('│  Sequential : {:.4f} с'.format(seq_time))
    if hadoop_top:
        print('│  Hadoop     : {:.4f} с'.format(hadoop_time))
    print('└' + '─' * (W - 2) + '┘')


def main():
    print('=== Word Count + Top K (Hadoop MapReduce Project) ===')
    print()
    print('1. Згенерувати вхідні дані автоматично')
    print('2. Використати існуючий файл')
    choice = input('Вибір [1/2]: ').strip() or '1'

    input_file = 'input.txt'

    if choice == '1':
        raw = input('Кількість слів [1000000]: ').strip()
        num_words = int(raw) if raw else 1000000
        raw_file = input('Назва файлу [input.txt]: ').strip()
        if raw_file:
            input_file = raw_file
        generate_data(num_words, input_file)
    else:
        raw_file = input('Шлях до файлу [input.txt]: ').strip()
        if raw_file:
            input_file = raw_file
        if not os.path.exists(input_file):
            print('Файл не знайдено: {}'.format(input_file))
            sys.exit(1)

    raw_k = input('Top K [20]: ').strip()
    k = int(raw_k) if raw_k else 20

    print()
    print('Запуск Sequential...')
    seq_top, seq_total, seq_unique, seq_time = run_sequential(input_file, k)
    print('Done: {:.4f} s'.format(seq_time))

    print('Запуск Hadoop (Docker)...')
    hadoop_top, hadoop_time = run_hadoop(input_file, k)
    if hadoop_top:
        print('Done: {:.4f} s'.format(hadoop_time))
    else:
        print('Hadoop недоступний.')

    print()
    print_results(input_file, k, seq_top, seq_total, seq_unique, seq_time, hadoop_top, hadoop_time)

    results_file = 'results.txt'
    save_results(results_file, input_file, k, seq_top, seq_total, seq_unique, seq_time, hadoop_top, hadoop_time)
    print()
    print('Результати збережено у {}'.format(results_file))


if __name__ == '__main__':
    main()
