def get_ncpus():
    from os import environ
    if 'OMP_NUM_THREADS' in environ:
        return int(environ['OMP_NUM_THREADS'])
    for ev in ['Q_CORE', 'LSB_MCPU_HOSTS']:
        if ev in environ:
            break
    else:
        return 1
    tokens = environ[ev].strip().split()
    if len(tokens) > 2:
        raise SystemError("Cannot handle this type of environment ({}='{}')".format(ev, environment[ev]))
    return int(tokens[1])

if __name__ == '__main__':
    print('Running with {} CPUS.'.format(get_ncpus()))
