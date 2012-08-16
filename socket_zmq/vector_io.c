#include <Python.h>

/* Includes needed for the sockaddr_* symbols below */

#ifdef __VMS
#   include <socket.h>
#else
#   include <sys/socket.h>
#endif

#include <netinet/in.h>
#include <sys/uio.h>
#if !(defined(__CYGWIN__) || (defined(PYOS_OS2) && defined(PYCC_VACPP)))
#  include <netinet/tcp.h>
#endif

#ifdef HAVE_SYS_UN_H
# include <sys/un.h>
#else
# undef AF_UNIX
#endif

#ifdef HAVE_LINUX_NETLINK_H
# ifdef HAVE_ASM_TYPES_H
#  include <asm/types.h>
# endif
# include <linux/netlink.h>
#else
#  undef AF_NETLINK
#endif

#ifdef HAVE_NET_IF_H
# include <net/if.h>
#endif

#ifdef HAVE_NETPACKET_PACKET_H
# include <sys/ioctl.h>
# include <netpacket/packet.h>
#endif

#ifdef HAVE_LINUX_CAN_H
#include <linux/can.h>
#endif

#ifdef HAVE_LINUX_CAN_RAW_H
#include <linux/can/raw.h>
#endif

#ifdef HAVE_SYS_SYS_DOMAIN_H
#include <sys/sys_domain.h>
#endif

#ifdef HAVE_SYS_KERN_CONTROL_H
#include <sys/kern_control.h>
#endif


/* Macros. */

#define SAS2SA(x)       (&((x)->sa))

#if INT_MAX > 0x7fffffff
#define SOCKLEN_T_LIMIT 0x7fffffff
#else
#define SOCKLEN_T_LIMIT INT_MAX
#endif

#include <stddef.h>

#ifndef offsetof
# define offsetof(type, member) ((size_t)(&((type *)0)->member))
#endif


/* The object holding a socket.  It holds some extra information,
   like the address family, which is used to decode socket address
   arguments properly. */

typedef struct {
    PyObject_HEAD
    int sock_fd;                /* Socket file descriptor */
    int sock_family;            /* Address family, e.g., AF_INET */
    PyObject *(*errorhandler)(void); /* Error handler; checks
                                        errno, returns NULL and
                                        sets a Python exception */
} PyVectorIOObject;


/* Socket address */

typedef union sock_addr {
    struct sockaddr_in in;
    struct sockaddr sa;
#ifdef AF_UNIX
    struct sockaddr_un un;
#endif
#ifdef AF_NETLINK
    struct sockaddr_nl nl;
#endif
#ifdef ENABLE_IPV6
    struct sockaddr_in6 in6;
    struct sockaddr_storage storage;
#endif
#ifdef HAVE_NETPACKET_PACKET_H
    struct sockaddr_ll ll;
#endif
#ifdef HAVE_SYS_KERN_CONTROL_H
    struct sockaddr_ctl ctl;
#endif
} sock_addr_t;


/* Get the address length according to the socket object's address family.
   Return 1 if the family is known, 0 otherwise.  The length is returned
   through len_ret. */

static int
getsockaddrlen(PyVectorIOObject *s, socklen_t *len_ret)
{
    switch (s->sock_family) {

#if defined(AF_UNIX)
    case AF_UNIX:
    {
        *len_ret = sizeof (struct sockaddr_un);
        return 1;
    }
#endif /* AF_UNIX */
#if defined(AF_NETLINK)
       case AF_NETLINK:
       {
           *len_ret = sizeof (struct sockaddr_nl);
           return 1;
       }
#endif

    case AF_INET:
    {
        *len_ret = sizeof (struct sockaddr_in);
        return 1;
    }

#ifdef ENABLE_IPV6
    case AF_INET6:
    {
        *len_ret = sizeof (struct sockaddr_in6);
        return 1;
    }
#endif

#ifdef HAVE_NETPACKET_PACKET_H
    case AF_PACKET:
    {
        *len_ret = sizeof (struct sockaddr_ll);
        return 1;
    }
#endif

    default:
        PyErr_SetString(PyExc_OSError, "getsockaddrlen: bad family");
        return 0;

    }
}


/* Support functions for the sendmsg() and recvmsg[_into]() methods.
   Currently, these methods are only compiled if the RFC 2292/3542
   CMSG_LEN() macro is available.  Older systems seem to have used
   sizeof(struct cmsghdr) + (length) where CMSG_LEN() is used now, so
   it may be possible to define CMSG_LEN() that way if it's not
   provided.  Some architectures might need extra padding after the
   cmsghdr, however, and CMSG_LEN() would have to take account of
   this. */

/* If length is in range, set *result to CMSG_LEN(length) and return
   true; otherwise, return false. */
static int
get_CMSG_LEN(size_t length, size_t *result)
{
    size_t tmp;

    if (length > (SOCKLEN_T_LIMIT - CMSG_LEN(0)))
        return 0;
    tmp = CMSG_LEN(length);
    if (tmp > SOCKLEN_T_LIMIT || tmp < length)
        return 0;
    *result = tmp;
    return 1;
}

#ifdef CMSG_SPACE
/* If length is in range, set *result to CMSG_SPACE(length) and return
   true; otherwise, return false. */
static int
get_CMSG_SPACE(size_t length, size_t *result)
{
    size_t tmp;

    /* Use CMSG_SPACE(1) here in order to take account of the padding
       necessary before *and* after the data. */
    if (length > (SOCKLEN_T_LIMIT - CMSG_SPACE(1)))
        return 0;
    tmp = CMSG_SPACE(length);
    if (tmp > SOCKLEN_T_LIMIT || tmp < length)
        return 0;
    *result = tmp;
    return 1;
}
#endif


/* Return true iff msg->msg_controllen is valid, cmsgh is a valid
   pointer in msg->msg_control with at least "space" bytes after it,
   and its cmsg_len member inside the buffer. */
static int
cmsg_min_space(struct msghdr *msg, struct cmsghdr *cmsgh, size_t space)
{
    size_t cmsg_offset;
    static const size_t cmsg_len_end = (offsetof(struct cmsghdr, cmsg_len) +
                                        sizeof(cmsgh->cmsg_len));

    /* Note that POSIX allows msg_controllen to be of signed type. */
    if (cmsgh == NULL || msg->msg_control == NULL || msg->msg_controllen < 0)
        return 0;
    if (space < cmsg_len_end)
        space = cmsg_len_end;
    cmsg_offset = (char *)cmsgh - (char *)msg->msg_control;
    return (cmsg_offset <= (size_t)-1 - space &&
            cmsg_offset + space <= msg->msg_controllen);
}


/* If pointer CMSG_DATA(cmsgh) is in buffer msg->msg_control, set
   *space to number of bytes following it in the buffer and return
   true; otherwise, return false.  Assumes cmsgh, msg->msg_control and
   msg->msg_controllen are valid. */
static int
get_cmsg_data_space(struct msghdr *msg, struct cmsghdr *cmsgh, size_t *space)
{
    size_t data_offset;
    char *data_ptr;

    if ((data_ptr = (char *)CMSG_DATA(cmsgh)) == NULL)
        return 0;
    data_offset = data_ptr - (char *)msg->msg_control;
    if (data_offset > msg->msg_controllen)
        return 0;
    *space = msg->msg_controllen - data_offset;
    return 1;
}


/* If cmsgh is invalid or not contained in the buffer pointed to by
   msg->msg_control, return -1.  If cmsgh is valid and its associated
   data is entirely contained in the buffer, set *data_len to the
   length of the associated data and return 0.  If only part of the
   associated data is contained in the buffer but cmsgh is otherwise
   valid, set *data_len to the length contained in the buffer and
   return 1. */
static int
get_cmsg_data_len(struct msghdr *msg, struct cmsghdr *cmsgh, size_t *data_len)
{
    size_t space, cmsg_data_len;

    if (!cmsg_min_space(msg, cmsgh, CMSG_LEN(0)) ||
        cmsgh->cmsg_len < CMSG_LEN(0))
        return -1;
    cmsg_data_len = cmsgh->cmsg_len - CMSG_LEN(0);
    if (!get_cmsg_data_space(msg, cmsgh, &space))
        return -1;
    if (space >= cmsg_data_len) {
        *data_len = cmsg_data_len;
        return 0;
    }
    *data_len = space;
    return 1;
}


/*
 * Call recvmsg() with the supplied iovec structures, flags, and
 * ancillary data buffer size (controllen).  Returns the tuple return
 * value for recvmsg() or recvmsg_into(), with the first item provided
 * by the supplied makeval() function.  makeval() will be called with
 * the length read and makeval_data as arguments, and must return a
 * new reference (which will be decrefed if there is a subsequent
 * error).  On error, closes any file descriptors received via
 * SCM_RIGHTS.
 */

static PyObject *
vector_io_recvmsg_guts(PyVectorIOObject *s, struct iovec *iov, int iovlen,
                       int flags, Py_ssize_t controllen,
                       PyObject *(*makeval)(ssize_t, void *), void *makeval_data)
{
    ssize_t bytes_received = -1;
    sock_addr_t addrbuf;
    socklen_t addrbuflen;
    struct msghdr msg = {0};
    PyObject *cmsg_list = NULL, *retval = NULL;
    void *controlbuf = NULL;
    struct cmsghdr *cmsgh;
    size_t cmsgdatalen = 0;
    int cmsg_status;

    /* XXX: POSIX says that msg_name and msg_namelen "shall be
       ignored" when the socket is connected (Linux fills them in
       anyway for AF_UNIX sockets at least).  Normally msg_namelen
       seems to be set to 0 if there's no address, but try to
       initialize msg_name to something that won't be mistaken for a
       real address if that doesn't happen. */
    if (!getsockaddrlen(s, &addrbuflen))
        return NULL;
    memset(&addrbuf, 0, addrbuflen);
    SAS2SA(&addrbuf)->sa_family = AF_UNSPEC;

    if (controllen < 0 || controllen > SOCKLEN_T_LIMIT) {
        PyErr_SetString(PyExc_ValueError,
                        "invalid ancillary data buffer length");
        return NULL;
    }
    if (controllen > 0 && (controlbuf = PyMem_Malloc(controllen)) == NULL)
        return PyErr_NoMemory();

    Py_BEGIN_ALLOW_THREADS;
    msg.msg_name = SAS2SA(&addrbuf);
    msg.msg_namelen = addrbuflen;
    msg.msg_iov = iov;
    msg.msg_iovlen = iovlen;
    msg.msg_control = controlbuf;
    msg.msg_controllen = controllen;
    bytes_received = recvmsg(s->sock_fd, &msg, flags);
    Py_END_ALLOW_THREADS;

    if (bytes_received < 0) {
        s->errorhandler();
        goto finally;
    }

    /* Make list of (level, type, data) tuples from control messages. */
    if ((cmsg_list = PyList_New(0)) == NULL)
        goto err_closefds;
    /* Check for empty ancillary data as old CMSG_FIRSTHDR()
       implementations didn't do so. */
    for (cmsgh = ((msg.msg_controllen > 0) ? CMSG_FIRSTHDR(&msg) : NULL);
         cmsgh != NULL; cmsgh = CMSG_NXTHDR(&msg, cmsgh)) {
        PyObject *bytes, *tuple;
        int tmp;

        cmsg_status = get_cmsg_data_len(&msg, cmsgh, &cmsgdatalen);
        if (cmsg_status != 0) {
            if (PyErr_WarnEx(PyExc_RuntimeWarning,
                             "received malformed or improperly-truncated "
                             "ancillary data", 1) == -1)
                goto err_closefds;
        }
        if (cmsg_status < 0)
            break;
        if (cmsgdatalen > PY_SSIZE_T_MAX) {
            PyErr_SetString(PyExc_OSError, "control message too long");
            goto err_closefds;
        }

        bytes = PyBytes_FromStringAndSize((char *)CMSG_DATA(cmsgh),
                                          cmsgdatalen);
        tuple = Py_BuildValue("iiN", (int)cmsgh->cmsg_level,
                              (int)cmsgh->cmsg_type, bytes);
        if (tuple == NULL)
            goto err_closefds;
        tmp = PyList_Append(cmsg_list, tuple);
        Py_DECREF(tuple);
        if (tmp != 0)
            goto err_closefds;

        if (cmsg_status != 0)
            break;
    }

    retval = Py_BuildValue("NOi",
                           (*makeval)(bytes_received, makeval_data),
                           cmsg_list,
                           (int)msg.msg_flags);

    if (retval == NULL)
        goto err_closefds;

finally:
    Py_XDECREF(cmsg_list);
    PyMem_Free(controlbuf);
    return retval;

err_closefds:
#ifdef SCM_RIGHTS
    /* Close all descriptors coming from SCM_RIGHTS, so they don't leak. */
    for (cmsgh = ((msg.msg_controllen > 0) ? CMSG_FIRSTHDR(&msg) : NULL);
         cmsgh != NULL; cmsgh = CMSG_NXTHDR(&msg, cmsgh)) {
        cmsg_status = get_cmsg_data_len(&msg, cmsgh, &cmsgdatalen);
        if (cmsg_status < 0)
            break;
        if (cmsgh->cmsg_level == SOL_SOCKET &&
            cmsgh->cmsg_type == SCM_RIGHTS) {
            size_t numfds;
            int *fdp;

            numfds = cmsgdatalen / sizeof(int);
            fdp = (int *)CMSG_DATA(cmsgh);
            while (numfds-- > 0)
                close(*fdp++);
        }
        if (cmsg_status != 0)
            break;
    }
#endif /* SCM_RIGHTS */
    goto finally;
}


/* s.recvmsg_into(buffers[, ancbufsize[, flags]]) method */

PyDoc_STRVAR(vector_io_recvmsg_into_doc,
"s.recvmsg_into(buffers[, ancbufsize[, flags]]) method");

static PyObject *
makeval_recvmsg_into(ssize_t received, void *data)
{
    return PyLong_FromSsize_t(received);
}

static PyObject *
vector_io_recvmsg_into(PyVectorIOObject *s, PyObject *args)
{
    Py_ssize_t ancbufsize = 0;
    int flags = 0;
    struct iovec *iovs = NULL;
    Py_ssize_t i, nitems, nbufs = 0;
    Py_buffer *bufs = NULL;
    PyObject *buffers_arg, *fast, *retval = NULL;

    if (!PyArg_ParseTuple(args, "O|ni:recvmsg_into",
                          &buffers_arg, &ancbufsize, &flags))
        return NULL;

    if ((fast = PySequence_Fast(buffers_arg,
                                "recvmsg_into() argument 1 must be an "
                                "iterable")) == NULL)
        return NULL;
    nitems = PySequence_Fast_GET_SIZE(fast);
    if (nitems > INT_MAX) {
        PyErr_SetString(PyExc_OSError, "recvmsg_into() argument 1 is too long");
        goto finally;
    }

    /* Fill in an iovec for each item, and save the Py_buffer
       structs to release afterwards. */
    if (nitems > 0 && ((iovs = PyMem_New(struct iovec, nitems)) == NULL ||
                       (bufs = PyMem_New(Py_buffer, nitems)) == NULL)) {
        PyErr_NoMemory();
        goto finally;
    }
    for (; nbufs < nitems; nbufs++) {
        if (!PyArg_Parse(PySequence_Fast_GET_ITEM(fast, nbufs),
                         "w*;recvmsg_into() argument 1 must be an iterable "
                         "of single-segment read-write buffers",
                         &bufs[nbufs]))
            goto finally;
        iovs[nbufs].iov_base = bufs[nbufs].buf;
        iovs[nbufs].iov_len = bufs[nbufs].len;
    }

    retval = vector_io_recvmsg_guts(s, iovs, nitems, flags, ancbufsize,
                                    &makeval_recvmsg_into, NULL);
finally:
    for (i = 0; i < nbufs; i++)
        PyBuffer_Release(&bufs[i]);
    PyMem_Free(bufs);
    PyMem_Free(iovs);
    Py_DECREF(fast);
    return retval;
}


/* s.sendmsg(buffers[, ancdata[, flags]]) method */

PyDoc_STRVAR(vector_io_sendmsg_doc,
"s.sendmsg(buffers[, ancdata[, flags]]) method");

static PyObject *
vector_io_sendmsg(PyVectorIOObject *s, PyObject *args)
{
    Py_ssize_t i, ndataparts, ndatabufs = 0, ncmsgs, ncmsgbufs = 0;
    Py_buffer *databufs = NULL;
    struct iovec *iovs = NULL;
    struct msghdr msg = {0};
    struct cmsginfo {
        int level;
        int type;
        Py_buffer data;
    } *cmsgs = NULL;
    void *controlbuf = NULL;
    size_t controllen, controllen_last;
    ssize_t bytes_sent = -1;
    int flags = 0;
    PyObject *data_arg, *cmsg_arg = NULL, *data_fast = NULL,
        *cmsg_fast = NULL, *retval = NULL;

    if (!PyArg_ParseTuple(args, "O|Oi:sendmsg",
                          &data_arg, &cmsg_arg, &flags))
        return NULL;

    /* Fill in an iovec for each message part, and save the Py_buffer
       structs to release afterwards. */
    if ((data_fast = PySequence_Fast(data_arg,
                                     "sendmsg() argument 1 must be an "
                                     "iterable")) == NULL)
        goto finally;
    ndataparts = PySequence_Fast_GET_SIZE(data_fast);
    if (ndataparts > INT_MAX) {
        PyErr_SetString(PyExc_OSError, "sendmsg() argument 1 is too long");
        goto finally;
    }
    msg.msg_iovlen = ndataparts;
    if (ndataparts > 0 &&
        ((msg.msg_iov = iovs = PyMem_New(struct iovec, ndataparts)) == NULL ||
         (databufs = PyMem_New(Py_buffer, ndataparts)) == NULL)) {
        PyErr_NoMemory();
        goto finally;
    }
    for (; ndatabufs < ndataparts; ndatabufs++) {
        if (!PyArg_Parse(PySequence_Fast_GET_ITEM(data_fast, ndatabufs),
                         "z*;sendmsg() argument 1 must be an iterable of "
                         "buffer-compatible objects",
                         &databufs[ndatabufs]))
            goto finally;
        iovs[ndatabufs].iov_base = databufs[ndatabufs].buf;
        iovs[ndatabufs].iov_len = databufs[ndatabufs].len;
    }

    if (cmsg_arg == NULL)
        ncmsgs = 0;
    else {
        if ((cmsg_fast = PySequence_Fast(cmsg_arg,
                                         "sendmsg() argument 2 must be an "
                                         "iterable")) == NULL)
            goto finally;
        ncmsgs = PySequence_Fast_GET_SIZE(cmsg_fast);
    }

#ifndef CMSG_SPACE
    if (ncmsgs > 1) {
        PyErr_SetString(PyExc_OSError,
                        "sending multiple control messages is not supported "
                        "on this system");
        goto finally;
    }
#endif
    /* Save level, type and Py_buffer for each control message,
       and calculate total size. */
    if (ncmsgs > 0 && (cmsgs = PyMem_New(struct cmsginfo, ncmsgs)) == NULL) {
        PyErr_NoMemory();
        goto finally;
    }
    controllen = controllen_last = 0;
    while (ncmsgbufs < ncmsgs) {
        size_t bufsize, space;

        if (!PyArg_Parse(PySequence_Fast_GET_ITEM(cmsg_fast, ncmsgbufs),
                         "(iiy*):[sendmsg() ancillary data items]",
                         &cmsgs[ncmsgbufs].level,
                         &cmsgs[ncmsgbufs].type,
                         &cmsgs[ncmsgbufs].data))
            goto finally;
        bufsize = cmsgs[ncmsgbufs++].data.len;

#ifdef CMSG_SPACE
        if (!get_CMSG_SPACE(bufsize, &space)) {
#else
        if (!get_CMSG_LEN(bufsize, &space)) {
#endif
            PyErr_SetString(PyExc_OSError, "ancillary data item too large");
            goto finally;
        }
        controllen += space;
        if (controllen > SOCKLEN_T_LIMIT || controllen < controllen_last) {
            PyErr_SetString(PyExc_OSError, "too much ancillary data");
            goto finally;
        }
        controllen_last = controllen;
    }

    /* Construct ancillary data block from control message info. */
    if (ncmsgbufs > 0) {
        struct cmsghdr *cmsgh = NULL;

        if ((msg.msg_control = controlbuf =
             PyMem_Malloc(controllen)) == NULL) {
            PyErr_NoMemory();
            goto finally;
        }
        msg.msg_controllen = controllen;

        /* Need to zero out the buffer as a workaround for glibc's
           CMSG_NXTHDR() implementation.  After getting the pointer to
           the next header, it checks its (uninitialized) cmsg_len
           member to see if the "message" fits in the buffer, and
           returns NULL if it doesn't.  Zero-filling the buffer
           ensures that that doesn't happen. */
        memset(controlbuf, 0, controllen);

        for (i = 0; i < ncmsgbufs; i++) {
            size_t msg_len, data_len = cmsgs[i].data.len;
            int enough_space = 0;

            cmsgh = (i == 0) ? CMSG_FIRSTHDR(&msg) : CMSG_NXTHDR(&msg, cmsgh);
            if (cmsgh == NULL) {
                PyErr_Format(PyExc_RuntimeError,
                             "unexpected NULL result from %s()",
                             (i == 0) ? "CMSG_FIRSTHDR" : "CMSG_NXTHDR");
                goto finally;
            }
            if (!get_CMSG_LEN(data_len, &msg_len)) {
                PyErr_SetString(PyExc_RuntimeError,
                                "item size out of range for CMSG_LEN()");
                goto finally;
            }
            if (cmsg_min_space(&msg, cmsgh, msg_len)) {
                size_t space;

                cmsgh->cmsg_len = msg_len;
                if (get_cmsg_data_space(&msg, cmsgh, &space))
                    enough_space = (space >= data_len);
            }
            if (!enough_space) {
                PyErr_SetString(PyExc_RuntimeError,
                                "ancillary data does not fit in calculated "
                                "space");
                goto finally;
            }
            cmsgh->cmsg_level = cmsgs[i].level;
            cmsgh->cmsg_type = cmsgs[i].type;
            memcpy(CMSG_DATA(cmsgh), cmsgs[i].data.buf, data_len);
        }
    }

    /* Make the system call. */
    Py_BEGIN_ALLOW_THREADS;
    bytes_sent = sendmsg(s->sock_fd, &msg, flags);
    Py_END_ALLOW_THREADS;

    if (bytes_sent < 0) {
        s->errorhandler();
        goto finally;
    }
    retval = PyLong_FromSsize_t(bytes_sent);

finally:
    PyMem_Free(controlbuf);
    for (i = 0; i < ncmsgbufs; i++)
        PyBuffer_Release(&cmsgs[i].data);
    PyMem_Free(cmsgs);
    Py_XDECREF(cmsg_fast);
    for (i = 0; i < ndatabufs; i++)
        PyBuffer_Release(&databufs[i]);
    PyMem_Free(databufs);
    PyMem_Free(iovs);
    Py_XDECREF(data_fast);
    return retval;
}


static PyMethodDef vector_io_methods[] = {
    { "recvmsg_into", (PyCFunction)vector_io_recvmsg_into, METH_VARARGS | METH_KEYWORDS,
            vector_io_recvmsg_into_doc },
    { "sendmsg", (PyCFunction)vector_io_sendmsg, METH_VARARGS,
            vector_io_sendmsg_doc },
    { NULL, NULL, 0, NULL } /* Sentinel */
};


/* Convenience function to raise an error according to errno
   and return a NULL pointer from a function. */

static PyObject *
set_error(void)
{
    return PyErr_SetFromErrno(PyExc_OSError);
}


/* Deallocate a socket object in response to the last Py_DECREF().
   First close the file description. */

static void
vector_io_dealloc(PyVectorIOObject *s)
{
    Py_TYPE(s)->tp_free((PyObject *)s);
}


static PyObject *
vector_io_repr(PyVectorIOObject *s)
{
    return PyUnicode_FromFormat(
        "<vector_io object, fd=%ld, family=%d>",
        (long)s->sock_fd, s->sock_family);
}

/* Create a new, uninitialized socket object. */

static PyObject *
vector_io_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    PyObject *new;

    new = type->tp_alloc(type, 0);
    if (new != NULL) {
        ((PyVectorIOObject *)new)->sock_fd = -1;
        ((PyVectorIOObject *)new)->errorhandler = &set_error;
    }
    return new;
}


/* Initialize a new socket object. */

/*ARGSUSED*/
static int
vector_io_initobj(PyObject *self, PyObject *args, PyObject *kwds)
{
    PyVectorIOObject *s = (PyVectorIOObject *)self;
    PyObject *fdobj = NULL;
    int fd = -1;
    int family = AF_INET;
    static char *keywords[] = {"family", "fileno", 0};

    if (!PyArg_ParseTupleAndKeywords(args, kwds,
                                     "iO:vector_io", keywords,
                                     &family, &fdobj))
        return -1;

    if (fdobj != NULL && fdobj != Py_None) {
        {
            fd = (int)PyLong_AsLong(fdobj);
            if (fd == -1 && PyErr_Occurred())
                return -1;
            if (fd == -1) {
                PyErr_SetString(PyExc_ValueError,
                                "can't use invalid socket value");
                return -1;
            }
        }
    }
    else {
        PyErr_SetString(PyExc_ValueError,
                        "can't use invalid socket value");
        return -1;
    }
    s->sock_fd = fd;
    s->sock_family = family;

    return 0;

}


/* Object documentation */
PyDoc_STRVAR(vector_io_doc,
"vector_io(family, fileno) -> socket object");


/* Type object for socket objects. */

static PyTypeObject vector_io_type = {
    PyVarObject_HEAD_INIT(0, 0)         /* Must fill in type value later */
    "vector_io.vector_io",                      /* tp_name */
    sizeof(PyVectorIOObject),                   /* tp_basicsize */
    0,                                          /* tp_itemsize */
    (destructor)vector_io_dealloc,              /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_reserved */
    (reprfunc)vector_io_repr,                   /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    PyObject_GenericGetAttr,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,   /* tp_flags */
    vector_io_doc,                              /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    vector_io_methods,                          /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    vector_io_initobj,                          /* tp_init */
    PyType_GenericAlloc,                        /* tp_alloc */
    vector_io_new,                              /* tp_new */
    PyObject_Del,                               /* tp_free */
};


PyDoc_STRVAR(vector_io_module_doc,
"Implementation module for iov socket operations.");


PyMODINIT_FUNC
initvector_io(void) {
    PyObject *m;

    Py_TYPE(&vector_io_type) = &PyType_Type;
    m = Py_InitModule3("vector_io",
                       NULL,
                       vector_io_module_doc);
    if (m == NULL)
        return;

    Py_INCREF((PyObject *)&vector_io_type);
    if (PyModule_AddObject(m, "vector_io",
                           (PyObject *)&vector_io_type) != 0)
        return;
}
