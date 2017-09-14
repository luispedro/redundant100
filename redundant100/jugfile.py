from os import makedirs
from jug import Task, TaskGenerator, barrier
from jug.utils import jug_execute
from glob import glob

@Task
def make():
    jug_execute.f(['make'])

@Task
def makeoutdirs():
    makedirs('partials/', exist_ok=True)
    makedirs('partials/copies', exist_ok=True)

@TaskGenerator
def sort_size(ifile):
    from os import path
    base = path.basename(ifile)
    ofile = f'partials/{base}.sorted.fna'
    jug_execute.f(['./bin/SortSizes', ifile, '-o', ofile])
    return ofile

def extract_block_size(fname):
    import re
    m  = re.match('^partials/splits\.(\d+)\.fna$', fname)
    return int(m.group(1), 10)

exec(open('./config.py').read())

input_sorted = sort_size(INPUT)
ofile_exact = 'partials/exact100.filtered.fna'
exact_copy_files = 'partials/copies/exact_0.txt'
r = jug_execute(['./bin/RemoveRepeats', input_sorted, '-o', ofile_exact, '-d', exact_copy_files])
jug_execute(['./bin/SplitBlocks', ofile_exact, '-d', 'partials/splits'], run_after=r)

barrier()
partials = glob('partials/splits/*.fna')
partials.sort(key=extract_block_size)

for i,p0 in partials:
    jug_execute.f(['./bin/FindExactOverlaps', p0, '-o', f'partials/copies/{i}_{i}.txt'])
    for j,p1 in enumerate(partials[i+1:]):
        jug_execute.f(['./bin/FindExactOverlaps', p0, p1, '-o', f'partials/copies/{i}_{j}.txt'])

