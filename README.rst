========================================================================
ThriftWorker - implements workers, transports, protocols for ThriftPool.
========================================================================

CI status: |cistatus|

`ThriftWorker` provides implementation of some primitives for `ThriftPool`_.

Key features:

* Support multiple workers implementation (sync, threads pool);
* Support multiple transports (currently only framed transport over TCP).

.. |cistatus| image:: https://secure.travis-ci.org/gdeetotdom/thriftworker.png?branch=master
.. _`ThriftPool`: https://github.com/blackwithwhite666/thriftpool
