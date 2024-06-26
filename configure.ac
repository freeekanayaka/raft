AC_PREREQ(2.60)
AC_INIT([raft], [0.22.1])
AC_LANG([C])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_AUX_DIR([ac])

AM_INIT_AUTOMAKE([subdir-objects -Wall -Wno-portability foreign])
AM_SILENT_RULES([yes])
AC_USE_SYSTEM_EXTENSIONS # Defines _GNU_SOURCE and similar

LT_INIT

# The libuv raft_io implementation is built by default if libuv is found, unless
# explicitly disabled.
AC_ARG_ENABLE(uv, AS_HELP_STRING([--disable-uv], [do not build the libuv-based raft_io implementation]))
AS_IF([test "x$enable_uv" != "xno"],
      [PKG_CHECK_MODULES(UV, [libuv >= 1.18.0], [have_uv=yes], [have_uv=no])],
      [have_uv=no])
AS_IF([test "x$enable_uv" = "xyes" -a "x$have_uv" = "xno"], [AC_MSG_ERROR([libuv required but not found])], [])
AM_CONDITIONAL(UV_ENABLED, test "x$have_uv" = "xyes")

# Automatically link to liblz4 if it's present.
#
# We try to detect if lz4 is installed only if the libuv raft_io implementation
# is enabled, since that's the only place where we use lz4.
AC_ARG_WITH([lz4], AS_HELP_STRING([--without-lz4], [never link to liblz4]))
AS_IF([test "x$have_uv" = "xyes"],
      # libuv is used
      [AS_IF([test "x$with_lz4" != "xno"],
             [PKG_CHECK_MODULES(LZ4, [liblz4 >= 1.7.1], [have_lz4=yes], [have_lz4=no])],
             [have_lz4=no])
       AS_IF([test "x$with_lz4" = "xyes" -a "x$have_lz4" = "xno"],
             [AC_MSG_ERROR([liblz4 required but not found])],
             [])],
      # libuv is not used
      [AS_IF([test "x$with_lz4" = "xyes"],
             [AC_MSG_ERROR([liblz4 can be used only if libuv is used too])],
             [])
       have_lz4=no])
AM_CONDITIONAL(LZ4_AVAILABLE, test "x$have_lz4" = "xyes")

AC_ARG_ENABLE(backtrace, AS_HELP_STRING([--enable-backtrace[=ARG]], [print backtrace on assertion failure [default=no]]))
AM_CONDITIONAL(BACKTRACE_ENABLED, test "x$enable_backtrace" = "xyes")

# The fake I/O implementation and associated fixture is built by default, unless
# explicitly disabled.
AC_ARG_ENABLE(fixture, AS_HELP_STRING([--disable-fixture], [do not build the raft_fixture test helper]))
AM_CONDITIONAL(FIXTURE_ENABLED, test "x$enable_fixture" != "xno")

# The example program is optional.
AC_ARG_ENABLE(example, AS_HELP_STRING([--enable-example[=ARG]], [build the example program [default=no]]))
AS_IF([test "x$enable_example" = "xyes" -a "x$have_uv" = "xno"], [AC_MSG_ERROR([example program requires libuv])], [])
AM_CONDITIONAL(EXAMPLE_ENABLED, test "x$enable_example" = "xyes")

# The benchmark programs are optional.
AC_ARG_ENABLE(benchmark, AS_HELP_STRING([--enable-benchmark[=ARG]], [build the benchmark programs [default=no]]))
AM_CONDITIONAL(BENCHMARK_ENABLED, test "x$enable_benchmark" = "xyes")

# Whether to enable debugging code.
AC_ARG_ENABLE(debug, AS_HELP_STRING([--enable-debug[=ARG]], [enable debugging [default=no]]))
AM_CONDITIONAL(DEBUG_ENABLED, test "x$enable_debug" = "xyes")

# Whether to enable compile-time hardening options.
AC_ARG_ENABLE(hardening, AS_HELP_STRING([--disable-hardening], [disable compile-time hardening options]))

# Whether to enable memory sanitizer.
AC_ARG_ENABLE(sanitize, AS_HELP_STRING([--enable-sanitize[=ARG]], [enable code sanitizers [default=no]]))
AM_CONDITIONAL(SANITIZE_ENABLED, test x"$enable_sanitize" = x"yes")

# Whether to enable compatibility with the legacy v0 API.
AC_ARG_ENABLE(v0, AS_HELP_STRING([--enable-v0[=ARG]], [enable compatibility with v0 API [default=yes]]))
AM_CONDITIONAL(V0_ENABLED, test "x$enable_v0" != "xno")

# Whether to enable code coverage.
AX_CODE_COVERAGE

# Check if lcov >= 2.0
AS_IF([test x"$enable_code_coverage" = xyes],
      [AX_COMPARE_VERSION($(lcov --version|cut -f 4 -d " " | cut -f 1 -d -), [ge], [2.0],
          [have_lcov_2=yes], [have_lcov_2=no])
      ],
      [have_lcov_2=no])
AM_CONDITIONAL(LCOV_VERSION_2, test x"$have_lcov_2" = x"yes")

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h string.h stdio.h assert.h unistd.h linux/io_uring.h linux/aio_abi.h])

# Checks for library functions and definitions.
AC_CHECK_DECLS(RWF_NOWAIT, [], [AC_MSG_ERROR(Linux kernel >= 4.14 required.)], [#include <linux/aio_abi.h>])

# Check if zfs >= 0.8.0 is available (for direct I/O support).
AC_CHECK_PROG(have_zfs, zfs, yes)
AS_IF([test x"$have_zfs" = x"yes"],
   [AX_COMPARE_VERSION($(cat /sys/module/zfs/version | cut -f 1 -d -), [ge], [0.8.0],
       [AC_DEFINE(RAFT_HAVE_ZFS_WITH_DIRECT_IO)], [])
   ],
   [])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT8_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T

# Enable large file support. This is mandatory in order to interoperate with
# libuv, which enables large file support by default, making the size of 'off_t'
# on 32-bit architecture be 8 bytes instead of the normal 4.
AC_SYS_LARGEFILE

CC_CHECK_FLAGS_APPEND([AM_CFLAGS],[CFLAGS],[ \
  -std=c11 \
  -g \
  -pipe \
])

AS_IF([test "x$enable_hardening" != "xno"],
      [CC_CHECK_FLAGS_APPEND([AM_CFLAGS],[CFLAGS],[ \
        -fcf-protection \
        --param=ssp-buffer-size=4 \
        -fdiagnostics-color \
        -fexceptions \
        -fstack-clash-protection \
        -fstack-protector-strong \
        -fasynchronous-unwind-tables \
        -fdiagnostics-show-option \
        -Wall \
        -Wextra \
        -Wpedantic \
        -Wimplicit-fallthrough=5 \
        -Wcast-align \
        -Wstrict-prototypes \
        -Wlogical-op \
        -Wmissing-include-dirs \
        -Wold-style-definition \
        -Winit-self \
        -Wfloat-equal \
        -Wsuggest-attribute=noreturn \
        -Wformat=2 \
        -Wendif-labels \
        -Wdate-time \
        -Wnested-externs \
        -Wconversion \
        -Werror=implicit-function-declaration \
        -Wunused-but-set-variable \
        -Werror=return-type \
        -Werror=incompatible-pointer-types \
        -Wshadow \
        -Werror=overflow \
        -Werror=shift-count-overflow \
        -Werror=shift-overflow=2 \
        -Warray-bounds \
        -Wrestrict \
        -Wreturn-local-addr \
        -Wstringop-overflow \
      ])],
      [])

AC_SUBST(AM_CFLAGS)

AS_IF([test "x$enable_hardening" != "xno"],
      [CC_CHECK_FLAGS_APPEND([AM_LDFLAGS],[LDFLAGS],[ \
        -z relro \
        -z now \
        -fstack-protector-strong \
        --param=ssp-buffer-size=4 \
      ])],
      [])

AC_SUBST(AM_LDLAGS)
AC_SUBST(enable_v0)

AC_CONFIG_FILES([include/raft.h raft.pc Makefile])
AC_OUTPUT
