from os import makedirs
from jug import Task, TaskGenerator, barrier
from jug.utils import jug_execute
from glob import glob
import subprocess

import ncpus

#@Task
def make():
    jug_execute.f(['make'])


@Task
def makeoutdirs():
    makedirs('partials/', exist_ok=True)
    makedirs('partials/copies', exist_ok=True)
    makedirs('partials/splits', exist_ok=True)
    makedirs('partials/filtered', exist_ok=True)

@TaskGenerator
def value_after(val, after):
    return val

@TaskGenerator
def sort_size(ifile):
    from os import path
    base = path.basename(ifile)
    ofile = f'partials/{base}.sorted.fna'
    jug_execute.f(['./bin/SortSizes', ifile, '-o', ofile])
    return ofile

def extract_block_size(fname):
    import re
    m  = re.match('^partials/splits/block\.(\d+)\.fna$', fname)
    return int(m.group(1), 10)

exec(open('./config.py').read())
@TaskGenerator
def find_overlaps(p, extra, oname):
    import tempfile
    import os
    n = tempfile.NamedTemporaryFile(mode='wt', delete=False)
    try:
        for e in extra:
            n.write(f'{e}\n')
        n.flush()
        n.close()
        subprocess.check_call([
            './bin/FindExactOverlaps',
            '-2',
            '-o', oname,
            '-i', p,
            '-e', n.name,
            '-j', str(ncpus.get_ncpus()),
            ])
    finally:
        os.unlink(n.name)

@TaskGenerator
def remove_duplicates(p, dups, oname):
    import tempfile
    import os
    n = tempfile.NamedTemporaryFile(mode='wt', delete=False)
    try:
        for e in dups:
            for d in open(e):
                d = d.strip()
                n.write(f'{d}\n')
        n.flush()
        n.close()
        subprocess.check_call([
            './bin/Remove',
            '-i', p,
            '-o', oname,
            '-d', n.name,
            ])
    finally:
        os.unlink(n.name)

def block(xs, n):
    while xs:
        yield xs[:n]
        xs = xs[n:]

input_sorted = sort_size(INPUT)
ofile_exact = 'partials/exact100.filtered.fna'
exact_copy_files = 'partials/copies/exact_0.txt'
r = jug_execute(['./bin/RemoveRepeats', input_sorted, '-o', ofile_exact, '-d', exact_copy_files])
jug_execute(['./bin/SplitBlocks', ofile_exact, '-d', 'partials/splits/block'], run_after=r)

barrier()
partials = glob('partials/splits/*.fna')
partials.sort(key=extract_block_size)

for i,p0 in enumerate(partials):
    oname = f'partials/copies/{i}_self.txt'
    r = jug_execute(['./bin/FindExactOverlaps', p0, '-o', oname])
    copies = [value_after(oname, after=r)]
    others = partials[i+1:]
    for j,chunk in enumerate(block(others, 8)):
        oname=f'partials/copies/{i}_{j}.txt'
        find_overlaps(p0, chunk, oname)
        copies.append(oname)
    remove_duplicates(p0, copies, p0.replace('/splits/', '/filtered/'))
