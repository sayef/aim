# distutils: language = c++
# cython: language_level = 3

from aim.storage cimport utils

# litewave is pure Python (SQLite + S3) and no longer
# ships a cimportable extension type. ContainerItemsIterator therefore defines
# the iterator protocol itself instead of extending an external Iterator.
cdef class ContainerItemsIterator:
    cdef object _current_value

    cpdef object next(self)
    cpdef object get(self)
    cpdef void skip(self)

cdef class Container:
    cdef __weakref__

    cpdef void close(self)
    cpdef void preload(self)
    cpdef object get(self, bytes key, object default = *)
    cpdef void set(self, bytes key, object value, store_batch = *)
    cpdef void delete_range(self, bytes begin, bytes end, store_batch = *)

    cpdef object items(self, bytes prefix = *)

    cpdef bytes next_key(self, bytes key = *)
    cpdef object next_value(self, bytes key = *)
    cpdef tuple next_key_value(self, bytes key = *)
    cpdef tuple next_item(self, bytes key = *)
    cpdef bytes prev_key(self, bytes key = *)
    cpdef object prev_value(self, bytes key = *)
    cpdef tuple prev_key_value(self, bytes key = *)
    cpdef tuple prev_item(self, bytes key = *)
