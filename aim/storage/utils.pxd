# distutils: language = c++
# cython: language_level = 3

# NOTE: litewave is a pure-Python package (SQLite + S3),
# so it no longer ships a cimportable extension type. ``interfaces`` is imported
# at the Python level in utils.py instead; ContainerItemsIterator is a plain
# Python class (see container.pxd / container.py).

cdef class ArrayFlagType:
    pass

cdef class ObjectFlagType:
    pass

cdef class CustomObjectFlagType:
    cdef:
        public str aim_name

cdef class BLOB:
    cdef:
        object data
        object loader_fn
    cpdef object load(self)
    # TODO closures inside cython functions are not supported yet
    # cpdef object transform(self, object transform_fn)
