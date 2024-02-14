.. _uv:

libuv driver
============

This driver integrates C-Raft's `core API`_ with `libuv`_. It's designed to be
generic enough to support both memory-based and disk-based state machines.

Network communication with other servers and persistence of log entries to disk
are entirely managed by the driver itself, while the application'sfinite state
machine and snapshot storing logic must be implemented by the user.

.. _core API: ./core.html
.. _libuv: http://libuv.org

Data types
----------

.. c:type:: uv_raft_t

    Handle that manages C-Raft's core engine.

.. c:type:: void (*uv_raft_commit_cb)(uv_raft_t *handle, raft_index index, int type, uv_buf_t *data)

    Callback invoked every time a new log entry gets committed. The user can
    process the entry data either synchronously or asynchronously. If data is
    processed asynchronously, and a new entry is committed while the previous
    one is still being processed, the user should queue up the new entry and
    process it when possible.
            
API
---

.. c:function:: int uv_raft_init(uv_loop_t *loop, uv_raft_t *handle)

    Initializes the C-Raft handle.

.. c:function:: int uv_raft_close(uv_raft_t *handle, uv_close_cb close_cb)

    Request C-Raft `handle` to be closed. The `close_cb` callback will be called
    asynchronously after this call. Moreover, the memory can only be released in
    `close_cb` or after it has returned.
