
set(RDKAFKA_PREFIX ${CMAKE_STATIC_LIBRARY_PREFIX})
set(RDKAFKA_SUFFIX ${CMAKE_STATIC_LIBRARY_SUFFIX})
set(RDKAFKA_LIBRARY_TYPE STATIC)

set(RdKafka_CPP_LIBNAME ${RDKAFKA_PREFIX}rdkafka++${RDKAFKA_SUFFIX})
set(RdKafka_C_LIBNAME ${RDKAFKA_PREFIX}rdkafka${RDKAFKA_SUFFIX})

find_path(RdKafka_INCLUDE_DIR
    NAMES librdkafka/rdkafka.h
    HINTS ${RdKafka_ROOT}/include
)

find_library(RdKafka_CPP_LIBRARY_PATH
    NAMES ${RdKafka_CPP_LIBNAME}
    HINTS ${RdKafka_ROOT}/lib ${RdKafka_ROOT}/lib64
)

find_library(RdKafka_C_LIBRARY_PATH
    NAMES ${RdKafka_C_LIBNAME}
    HINTS ${RdKafka_ROOT}/lib ${RdKafka_ROOT}/lib64
)


add_library(RdKafka::rdkafka ${RDKAFKA_LIBRARY_TYPE} IMPORTED GLOBAL)
add_library(RdKafka::rdkafka++ ${RDKAFKA_LIBRARY_TYPE} IMPORTED GLOBAL)

set(RDKAFKA_DEPENDENCIES pthread rt ssl crypto dl z)

set_target_properties(RdKafka::rdkafka PROPERTIES
    IMPORTED_NAME RdKafka
    IMPORTED_LOCATION "${RdKafka_C_LIBRARY_PATH}"
    INTERFACE_INCLUDE_DIRECTORIES "${RdKafka_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES  "${RDKAFKA_DEPENDENCIES}")

set_target_properties(RdKafka::rdkafka++ PROPERTIES
    IMPORTED_NAME RdKafka
    IMPORTED_LOCATION "${RdKafka_CPP_LIBRARY_PATH}"
    INTERFACE_INCLUDE_DIRECTORIES "${RdKafka_INCLUDE_DIR}" 
    INTERFACE_LINK_LIBRARIES "${RDKAFKA_DEPENDENCIES}")

message(STATUS "Found valid rdkafka libs")
mark_as_advanced(
	RDKAFKA_LIBRARY
	RdKafka_INCLUDE_DIR
	RdKafka_C_LIBRARY_PATH
	RdKafka_CPP_LIBRARY_PATH)
