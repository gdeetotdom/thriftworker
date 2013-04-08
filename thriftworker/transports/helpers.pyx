from os import strerror
from libc cimport errno
from libc.string cimport memset
from cpython.int cimport PyInt_Check
from cpython.bytes cimport PyBytes_Size
from cpython.string cimport PyString_FromString


cdef extern from "arpa/inet.h":
    cdef enum:
        INET_ADDRSTRLEN
        INET6_ADDRSTRLEN

    int htons (int)
    int htonl (int)
    int ntohl (int)
    int ntohs (int)


cdef extern from "stdint.h":

    ctypedef unsigned int uint8_t
    ctypedef unsigned int uint16_t
    ctypedef unsigned int uint32_t
    ctypedef unsigned int uint64_t


cdef extern from "sys/un.h":
    IF UNAME_SYSNAME == "Linux":
        cdef struct sockaddr_un:
            short sun_family
            char sun_path[104]
    ELSE:
        pass


cdef extern from "sys/socket.h":
    int AF_UNSPEC, AF_INET, AF_INET6, AF_UNIX
    int SOCK_STREAM, SOCK_DGRAM, SOL_SOCKET, INADDR_ANY
    int SHUT_RD, SHUT_WR, SHUT_RDWR

    int SO_DEBUG, SO_REUSEADDR, SO_KEEPALIVE, SO_DONTROUTE, SO_LINGER
    int SO_BROADCAST, SO_OOBINLINE, SO_SNDBUF, SO_RCVBUF, SO_SNDLOWAT
    int SO_RCVLOWAT, SO_SNDTIMEO, SO_RCVTIMEO, SO_TYPE, SO_ERROR
    IF UNAME_SYSNAME == "FreeBSD":
        int SO_REUSEPORT, SO_ACCEPTFILTER

    int SO_DONTROUTE, SO_LINGER, SO_BROADCAST, SO_OOBINLINE, SO_SNDBUF
    int SO_REUSEADDR, SO_DEBUG, SO_RCVBUF, SO_SNDLOWAT, SO_RCVLOWAT
    int SO_SNDTIMEO, SO_RCVTIMEO, SO_KEEPALIVE, SO_TYPE, SO_ERROR

    ctypedef unsigned int sa_family_t
    ctypedef unsigned int in_port_t
    ctypedef unsigned int in_addr_t
    ctypedef unsigned int socklen_t

    cdef struct in_addr:
        in_addr_t s_addr

    union ip__u6_addr:
        uint8_t  __u6_addr8[16]
        uint16_t __u6_addr16[8]
        uint32_t __u6_addr32[4]

    struct in6_addr:
        ip__u6_addr __u6_addr

    IF UNAME_SYSNAME == "FreeBSD" or UNAME_SYSNAME == "Darwin":
        cdef struct sockaddr:
            unsigned char sa_len
            sa_family_t sa_family
            char sa_data[250]

        cdef struct sockaddr_in:
            unsigned char sin_len
            sa_family_t sin_family
            in_port_t sin_port
            in_addr sin_addr
            char sin_zero[8]

        cdef struct sockaddr_in6:
            unsigned char sin6_len
            sa_family_t sin6_family
            in_port_t sin6_port
            unsigned int sin6_flowinfo
            in6_addr sin6_addr
            unsigned int sin6_scope_id

        cdef struct sockaddr_un:
            unsigned char sun_len
            sa_family_t sun_family
            char sun_path[104]

        cdef struct sockaddr_storage:
            unsigned char sa_len
            sa_family_t sa_family
    ELSE:
        cdef struct sockaddr:
            sa_family_t sa_family
            char sa_data[250]

        cdef struct sockaddr_in:
            sa_family_t sin_family
            unsigned short sin_port
            in_addr sin_addr
            char sa_data[250]

        cdef struct sockaddr_in6:
            sa_family_t sin6_family
            unsigned short sin6_port
            in6_addr sin6_addr
            char sa_data[250]

        cdef struct sockaddr_storage:
            sa_family_t sa_family
            char sa_data[250]

    int socket      (int domain, int type, int protocol)
    int connect     (int fd, sockaddr * addr, socklen_t addr_len)
    int accept      (int fd, sockaddr * addr, socklen_t * addr_len)
    int bind        (int fd, sockaddr * addr, socklen_t addr_len)
    int listen      (int fd, int backlog)
    int shutdown    (int fd, int how)
    int close       (int fd)
    int getsockopt  (int fd, int level, int optname, void * optval, socklen_t * optlen)
    int setsockopt  (int fd, int level, int optname, void * optval, socklen_t optlen)
    int getpeername (int fd, sockaddr * name, socklen_t * namelen)
    int getsockname (int fd, sockaddr * name, socklen_t * namelen)
    int sendto      (int fd, void * buf, size_t len, int flags, sockaddr * addr, socklen_t addr_len)
    int send        (int fd, void * buf, size_t len, int flags)
    int recv        (int fd, void * buf, size_t len, int flags)
    int recvfrom    (int fd, void * buf, size_t len, int flags, sockaddr * addr, socklen_t * addr_len)
    int _c_socketpair "socketpair"  (int d, int type, int protocol, int *sv)
    int inet_pton   (int af, char *src, void *dst)
    char *inet_ntop (int af, void *src, char *dst, socklen_t size)
    char * inet_ntoa (in_addr pin)
    int inet_aton   (char * cp, in_addr * pin)


cdef extern from "fcntl.h":
    int fcntl (int fd, int cmd, ...)
    int F_GETFL, O_NONBLOCK, F_SETFL


cdef object unparse_address(sockaddr_storage *sa, socklen_t addr_len):
    """Unpack a C-socket address structure and generate a Python address object.

    :param sa: The sockaddr_storage structure to unpack.
    :param addr_len: The length of the ``sa`` structure.

    :returns: A ``(IP, port)`` tuple for IP addresses where IP is a
        string in canonical format for the given address family . Returns a
        string for UNIX-domain sockets.  Returns None for unknown socket
        domains.
    """
    cdef sockaddr_in * sin
    cdef sockaddr_in6 *sin6
    cdef sockaddr_un * sun
    cdef char ascii_buf[INET6_ADDRSTRLEN]

    if (<sockaddr_in *>sa).sin_family == AF_INET:
        sin = <sockaddr_in *> sa
        inet_ntop (AF_INET, &(sin.sin_addr), ascii_buf, INET_ADDRSTRLEN)
        return (PyString_FromString(ascii_buf), ntohs(sin.sin_port))
    elif (<sockaddr_in6 *>sa).sin6_family == AF_INET6:
        sin6 = <sockaddr_in6 *> sa
        inet_ntop (AF_INET6, &(sin6.sin6_addr), ascii_buf, INET6_ADDRSTRLEN)
        return (PyString_FromString(ascii_buf), ntohs(sin6.sin6_port))
    elif (<sockaddr_un *>sa).sun_family == AF_UNIX:
        sun = <sockaddr_un *>sa
        return sun.sun_path
    else:
        return None


def raise_oserror(int error_number):
    """Raise an OSError exception by errno."""
    raise OSError(error_number, strerror(error_number))


def accept_connection(int fd):
    """Accept a connection.

    :returns: A tuple ``(socket, address)`` where ``socket`` is a socket
        object and ``address`` is an ``(IP, port)`` tuple for IP
        addresses or a string for UNIX-domain sockets. IP addresses are
        returned as strings.

    :raises OSError: OS-level error.
    """
    cdef sockaddr_storage sa
    cdef socklen_t addr_len
    cdef int r

    memset(&sa, 0, sizeof(sockaddr_storage))
    addr_len = sizeof(sockaddr_storage)
    r = accept(fd, <sockaddr *>&sa, &addr_len)

    if r == -1:
        raise_oserror(errno.errno)

    return (r, unparse_address(&sa, addr_len))


def set_nonblocking(int fd):
    """Make descriptor non-blocking."""
    cdef int flag
    flag = fcntl(fd, F_GETFL, 0)
    if flag == -1:
        raise_oserror(errno.errno)
    elif fcntl(fd, F_SETFL, flag | O_NONBLOCK) == -1:
        raise_oserror(errno.errno)


def set_sockopt(int fd, int level, int optname, value):
    """Set a socket option.

    :param level: The socket level to set (see :class:`SOL`).
    :param optname: The socket option to set (see :class:`SO`).
    :param value: The value to set.  May be an integer, or a struct-packed string.

    :raises OSError: OS-level error.

    """
    cdef int flag, r
    cdef socklen_t optlen
    if PyInt_Check(value):
        flag = value
        r = setsockopt(fd, level, optname, <void*>&flag, sizeof (flag))
    else:
        optlen = PyBytes_Size (value) # does typecheck
        r = setsockopt(fd, level, optname, <void*>value, optlen)
    if r == -1:
        raise_oserror(errno.errno)
