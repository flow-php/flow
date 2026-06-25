dnl config.m4 for extension pg_query

PHP_ARG_WITH([pg-query],
  [whether to enable pg_query support],
  [AS_HELP_STRING([--with-pg-query@<:@=DIR@:>@],
    [Include pg_query support. DIR is the libpg_query install prefix (optional - will download if not found)])])

if test "$PHP_PG_QUERY" != "no"; then
  dnl libpg_query 18-latest is required for postgres_deparse.h support
  LIBPG_QUERY_VERSION="18-latest"
  AC_MSG_NOTICE([Using libpg_query $LIBPG_QUERY_VERSION (PostgreSQL 18 grammar)])

  PG_QUERY_DIR=""

  dnl Expected PostgreSQL major derived from the pinned version ("18-latest" -> "18", "18.0.0" -> "18")
  PG_EXPECTED_MAJOR=`echo "$LIBPG_QUERY_VERSION" | sed -E 's/^([[0-9]]+).*/\1/'`

  dnl Search for existing libpg_query installation
  if test "$PHP_PG_QUERY" != "yes" && test -n "$PHP_PG_QUERY"; then
    SEARCH_PATH="$PHP_PG_QUERY"
    PG_QUERY_EXPLICIT="yes"
  else
    SEARCH_PATH="/usr/local /usr /opt/local /opt/homebrew"
    PG_QUERY_EXPLICIT="no"
  fi

  AC_MSG_CHECKING([for libpg_query])

  for i in $SEARCH_PATH ; do
    dnl Locate headers + static lib in either flat or include/lib layout
    _hdr=""; _inc=""; _lib=""
    if test -r "$i/pg_query.h" && test -r "$i/postgres_deparse.h" && test -r "$i/libpg_query.a"; then
      _hdr="$i/pg_query.h"; _inc="$i"; _lib="$i"
    elif test -r "$i/include/pg_query.h" && test -r "$i/include/postgres_deparse.h" && test -r "$i/lib/libpg_query.a"; then
      _hdr="$i/include/pg_query.h"; _inc="$i/include"; _lib="$i/lib"
    fi

    if test -z "$_hdr"; then
      continue
    fi

    dnl PG major is a macro in the header we already require: #define PG_MAJORVERSION "18"
    _found_major=`sed -nE 's/^#define[[:space:]]+PG_MAJORVERSION[[:space:]]+"([0-9]+)".*/\1/p' "$_hdr"`

    if test "x$_found_major" = "x$PG_EXPECTED_MAJOR"; then
      PG_QUERY_DIR="$i"
      PG_QUERY_INCLUDE_DIR="$_inc"
      PG_QUERY_LIB_DIR="$_lib"
      AC_MSG_RESULT([found in $i (PostgreSQL $_found_major)])
      break
    elif test "$PG_QUERY_EXPLICIT" = "yes"; then
      AC_MSG_RESULT([version mismatch])
      AC_MSG_ERROR([libpg_query in $i is for PostgreSQL ${_found_major:-unknown}, but this extension targets PostgreSQL $PG_EXPECTED_MAJOR (libpg_query $LIBPG_QUERY_VERSION). Point --with-pg-query at a matching install, or omit it to download the pinned version.])
    else
      AC_MSG_WARN([ignoring libpg_query in $i: PostgreSQL ${_found_major:-unknown}, need $PG_EXPECTED_MAJOR])
    fi
  done

  dnl Get the absolute path of the extension source directory
  EXT_DIR=`pwd`

  dnl Check bundled directory
  if test -z "$PG_QUERY_DIR"; then
    if test -r "$EXT_DIR/libpg_query/pg_query.h" && test -r "$EXT_DIR/libpg_query/postgres_deparse.h" && test -r "$EXT_DIR/libpg_query/libpg_query.a"; then
      PG_QUERY_DIR="$EXT_DIR/libpg_query"
      AC_MSG_RESULT([using bundled libpg_query])
    fi
  fi

  dnl Download and build if not found
  if test -z "$PG_QUERY_DIR"; then
    AC_MSG_RESULT([not found, will download and build])

    AC_MSG_CHECKING([for git])
    if test -z "$GIT"; then
      AC_PATH_PROG(GIT, git, no)
    fi
    if test "$GIT" = "no"; then
      AC_MSG_ERROR([git is required to download libpg_query])
    fi
    AC_MSG_RESULT([$GIT])

    AC_MSG_CHECKING([for make])
    if test -z "$MAKE"; then
      AC_PATH_PROG(MAKE, make, no)
    fi
    if test "$MAKE" = "no"; then
      AC_MSG_ERROR([make is required to build libpg_query])
    fi
    AC_MSG_RESULT([$MAKE])

    LIBPG_QUERY_BUILD_DIR="$EXT_DIR/libpg_query"

    AC_MSG_NOTICE([Downloading libpg_query $LIBPG_QUERY_VERSION...])
    if test -d "$LIBPG_QUERY_BUILD_DIR"; then
      rm -rf "$LIBPG_QUERY_BUILD_DIR"
    fi

    $GIT clone --depth=1 --branch=$LIBPG_QUERY_VERSION \
      https://github.com/pganalyze/libpg_query.git "$LIBPG_QUERY_BUILD_DIR" || \
      AC_MSG_ERROR([Failed to download libpg_query])

    AC_MSG_NOTICE([Building libpg_query...])
    (cd "$LIBPG_QUERY_BUILD_DIR" && $MAKE -j) || \
      AC_MSG_ERROR([Failed to build libpg_query])

    PG_QUERY_DIR="$LIBPG_QUERY_BUILD_DIR"
  fi

  dnl Set include and lib directories
  if test -z "$PG_QUERY_INCLUDE_DIR"; then
    PG_QUERY_INCLUDE_DIR="$PG_QUERY_DIR"
  fi
  if test -z "$PG_QUERY_LIB_DIR"; then
    PG_QUERY_LIB_DIR="$PG_QUERY_DIR"
  fi

  dnl Add include paths
  PHP_ADD_INCLUDE($PG_QUERY_INCLUDE_DIR)

  dnl Check for static library
  if test -r "$PG_QUERY_LIB_DIR/libpg_query.a"; then
    dnl Static linking - embed the library
    LDFLAGS="$LDFLAGS $PG_QUERY_LIB_DIR/libpg_query.a"
    AC_DEFINE(HAVE_PG_QUERY, 1, [Whether you have libpg_query])
  else
    AC_MSG_ERROR([libpg_query.a not found in $PG_QUERY_LIB_DIR])
  fi

  dnl protobuf-c is required for shared builds (libpg_query.a needs it)
  if test "$ext_shared" = "yes"; then
    AC_MSG_CHECKING([for protobuf-c])

    dnl Try pkg-config first (the standard way to find libraries)
    if test -z "$PKG_CONFIG"; then
      AC_PATH_PROG(PKG_CONFIG, pkg-config, no)
    fi

    if test "$PKG_CONFIG" != "no" && $PKG_CONFIG --exists libprotobuf-c 2>/dev/null; then
      PROTOBUF_C_LIBS=$($PKG_CONFIG --libs libprotobuf-c)
      PROTOBUF_C_LIBDIR=$($PKG_CONFIG --variable=libdir libprotobuf-c)
      AC_MSG_RESULT([found via pkg-config])

      if test -n "$PROTOBUF_C_LIBDIR"; then
        PHP_ADD_LIBPATH($PROTOBUF_C_LIBDIR, PG_QUERY_SHARED_LIBADD)
      fi
      PHP_ADD_LIBRARY(protobuf-c,, PG_QUERY_SHARED_LIBADD)
    else
      dnl Fallback: search common paths (for systems without pkg-config)
      PROTOBUF_C_SEARCH_PATHS="/opt/homebrew /usr/local /usr"
      PROTOBUF_C_FOUND=""

      for i in $PROTOBUF_C_SEARCH_PATHS; do
        if test -r "$i/lib/libprotobuf-c.dylib" || test -r "$i/lib/libprotobuf-c.so"; then
          PROTOBUF_C_FOUND=$i
          break
        fi
      done

      if test -n "$PROTOBUF_C_FOUND"; then
        AC_MSG_RESULT([found in $PROTOBUF_C_FOUND])
        PHP_ADD_LIBPATH($PROTOBUF_C_FOUND/lib, PG_QUERY_SHARED_LIBADD)
        PHP_ADD_LIBRARY(protobuf-c,, PG_QUERY_SHARED_LIBADD)
      else
        AC_MSG_RESULT([not found, assuming system default])
        PHP_ADD_LIBRARY(protobuf-c,, PG_QUERY_SHARED_LIBADD)
      fi
    fi
  fi

  PHP_SUBST(PG_QUERY_SHARED_LIBADD)

  dnl Define extension
  PHP_NEW_EXTENSION(pg_query, pg_query.c, $ext_shared,, -DZEND_ENABLE_STATIC_TSRMLS_CACHE=1)

  dnl macOS libtool fix for flat namespace issue
  dnl libpg_query.a bundles its own copy of protobuf-c. On macOS, libtool defaults to
  dnl -flat_namespace which pools all symbols together. If system protobuf-c is also loaded
  dnl (e.g., via grpc extension), symbol conflicts cause segfaults. This fix keeps the
  dnl two-level namespace so bundled symbols stay isolated.
  dnl See: https://bugs.php.net/80393, https://github.com/protocolbuffers/protobuf/issues/7611
  case $host_os in
    darwin*)
      AC_CONFIG_COMMANDS([libtool-macos-fix], [
        if test -f libtool; then
          sed -i.bak 's/.*flat_namespace.*suppress.*/allow_undefined_flag="-undefined dynamic_lookup"/' libtool
          rm -f libtool.bak
        fi
      ])
      ;;
  esac
fi
