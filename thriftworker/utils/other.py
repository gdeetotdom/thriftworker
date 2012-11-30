"""Some other useful tools."""
from __future__ import absolute_import

import itertools


def rgetattr(obj, path):
    """Get nested attribute from object.

    :param obj: object
    :param path: path to attribute

    """
    return reduce(getattr, [obj] + path.split('.'))


def get_port_from_range(name, range_start, range_end):
    """Get random port from given range [range_start, range_end]."""
    # Detect port for this service.
    hashed_port = abs(hash(name) & 0xffff)
    # Ensure that detected port is in pool.
    while hashed_port > range_start:
        hashed_port = hashed_port - range_start
    # If port is greater than allowed.
    while range_end < hashed_port + range_start:
        hashed_port = hashed_port // 2
    return hashed_port + range_start


def get_addresses_from_pool(name, address, port_range=None):
    """Get addresses from pool."""
    host, port = address
    if isinstance(port, int):
        ports = (port or 0,)
    elif isinstance(port, basestring) or port is None:
        if port and port.isdigit():
            ports = (int(port),)
        elif port == 'random' or (port is None and port_range is None):
            ports = (0,)
        elif port_range is None and port:
            raise ValueError('Port range not specified')
        elif port is None or port == 'pool':
            range_start, range_end = port_range[0], port_range[1]
            default_port = get_port_from_range(name, range_start, range_end)
            chains = [(default_port,),
                      xrange(default_port + 1, range_end),
                      xrange(range_start, default_port)]
            ports = itertools.chain.from_iterable(chains)
        else:
            raise ValueError('Unknown port {0!r}'.format(port))
    else:
        raise ValueError('Unknown address {0!r}'.format(address))
    for port in ports:
        yield (host, port)
