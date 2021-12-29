#!/usr/bin/env python

import os
import subprocess
import sys
import threading

# Simplified, non-threadsafe version for force_align.py
# Use the version in realtime for development

## how to use? (jeonghyeok.park)
## first, make model file and error file
# fast_align -d -v -o -p path/to/forward_model > path/to/forward_alignment 2> path/to/forward_error
# fast_align -d -v -o -p -r path/to/backward_model > path/to/backward_alignment 2> path/to/backward_error
## trained model로 새로운 corpus alignment하기
## 활용 방법: clean한 데이터로 alignment model 훈련 후 noisy data를 alignment score로 filtering 하기
# python force_align.py path/to/forward_model path/to/forward_error path/to/backward_model path/to/backward_error [heuristic option] < path/to/input_parallel_data > path/to/output_parallel_data
# [heuristic option, default=grow-diag-final-and] = [intersect, union, grow-diag, grow-diag-final, grow-diag-final-and]

class Aligner:

    def __init__(self, fwd_params, fwd_err, rev_params, rev_err, heuristic='grow-diag-final-and'):

        build_root = os.path.dirname(os.path.abspath(__file__))
        fast_align = os.path.join(build_root, 'fast_align')
        atools = os.path.join(build_root, 'atools')

        (fwd_T, fwd_m) = self.read_err(fwd_err)
        (rev_T, rev_m) = self.read_err(rev_err)

        fwd_cmd = [fast_align, '-i', '-', '-d', '-s','-T', fwd_T, '-m', fwd_m, '-f', fwd_params]
        rev_cmd = [fast_align, '-i', '-', '-d', '-s','-T', rev_T, '-m', rev_m, '-f', rev_params, '-r']
        tools_cmd = [atools, '-i', '-', '-j', '-', '-c', heuristic]

        self.fwd_align = popen_io(fwd_cmd)
        self.rev_align = popen_io(rev_cmd)
        self.tools = popen_io(tools_cmd)

    def align(self, line):
        self.fwd_align.stdin.write('{}\n'.format(line).encode('utf-8'))
        self.rev_align.stdin.write('{}\n'.format(line).encode('utf-8'))
        # f words ||| e words ||| links ||| score
        fwd_line = self.fwd_align.stdout.readline().split('|||')[2].strip()
        rev_line = self.rev_align.stdout.readline().split('|||')[2].strip()
        self.tools.stdin.write('{}\n'.format(fwd_line))
        self.tools.stdin.write('{}\n'.format(rev_line))
        al_line = self.tools.stdout.readline().strip()
        return al_line
 
    def close(self):
        self.fwd_align.stdin.close()
        self.fwd_align.wait()
        self.rev_align.stdin.close()
        self.rev_align.wait()
        self.tools.stdin.close()
        self.tools.wait()

    def read_err(self, err):
        (T, m) = ('', '')
        for line in open(err):
            # expected target length = source length * N
            if 'expected target length' in line:
                m = line.split()[-1]
            # final tension: N
            elif 'final tension' in line:
                T = line.split()[-1]
        return (T, m)

def popen_io(cmd):
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    def consume(s):
        for _ in s:
            pass
    threading.Thread(target=consume, args=(p.stderr,)).start()
    return p

def main():

    if len(sys.argv[1:]) < 4:
        sys.stderr.write('run:\n')
        sys.stderr.write('  fast_align -i corpus.f-e -d -v -o -p fwd_params >fwd_align 2>fwd_err\n')
        sys.stderr.write('  fast_align -i corpus.f-e -r -d -v -o -p rev_params >rev_align 2>rev_err\n')
        sys.stderr.write('\n')
        sys.stderr.write('then run:\n')
        sys.stderr.write('  {} fwd_params fwd_err rev_params rev_err [heuristic] <in.f-e >out.f-e.gdfa\n'.format(sys.argv[0]))
        sys.stderr.write('\n')
        sys.stderr.write('where heuristic is one of: (intersect union grow-diag grow-diag-final grow-diag-final-and) default=grow-diag-final-and\n')
        sys.exit(2)

    aligner = Aligner(*sys.argv[1:])

    while True:
        line = sys.stdin.readline()
        if not line:
            break
        sys.stdout.write('{}\n'.format(aligner.align(line.strip())))
        sys.stdout.flush()

    aligner.close()
    
if __name__ == '__main__':
    main()


