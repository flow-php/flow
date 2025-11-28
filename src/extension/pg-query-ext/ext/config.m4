dnl config.m4 for extension pg_query

PHP_ARG_WITH([pg-query],
  [whether to enable pg_query support],
  [AS_HELP_STRING([--with-pg-query@<:@=DIR@:>@],
    [Include pg_query support. DIR is the libpg_query install prefix (optional - will download if not found)])])

PHP_ARG_WITH([pg-version],
  [PostgreSQL grammar version],
  [AS_HELP_STRING([--with-pg-version@<:@=VERSION@:>@],
    [PostgreSQL grammar version: 15, 16, or 17 (default: 17)])], [17], [no])

if test "$PHP_PG_QUERY" != "no"; then
  dnl Map PostgreSQL version to libpg_query version
  case "$PHP_PG_VERSION" in
    15)
      LIBPG_QUERY_VERSION="15-4.2.4"
      ;;
    16)
      LIBPG_QUERY_VERSION="16-5.2.0"
      ;;
    17|yes|"")
      LIBPG_QUERY_VERSION="17-latest"
      PHP_PG_VERSION="17"
      ;;
    *)
      AC_MSG_ERROR([Unsupported PostgreSQL version: $PHP_PG_VERSION. Supported versions: 15, 16, 17])
      ;;
  esac

  AC_MSG_NOTICE([Using PostgreSQL $PHP_PG_VERSION grammar (libpg_query $LIBPG_QUERY_VERSION)])

  PG_QUERY_DIR=""

  dnl Search for existing libpg_query installation
  if test "$PHP_PG_QUERY" != "yes" && test -n "$PHP_PG_QUERY"; then
    SEARCH_PATH="$PHP_PG_QUERY"
  else
    SEARCH_PATH="/usr/local /usr /opt/local /opt/homebrew"
  fi

  AC_MSG_CHECKING([for libpg_query])

  for i in $SEARCH_PATH ; do
    if test -r "$i/pg_query.h" && test -r "$i/libpg_query.a"; then
      PG_QUERY_DIR=$i
      AC_MSG_RESULT([found in $i])
      break
    fi
    if test -r "$i/include/pg_query.h" && test -r "$i/lib/libpg_query.a"; then
      PG_QUERY_DIR=$i
      PG_QUERY_INCLUDE_DIR="$i/include"
      PG_QUERY_LIB_DIR="$i/lib"
      AC_MSG_RESULT([found in $i])
      break
    fi
  done

  dnl Get the absolute path of the extension source directory
  EXT_DIR=`pwd`

  dnl Check bundled directory
  if test -z "$PG_QUERY_DIR"; then
    if test -r "$EXT_DIR/libpg_query/pg_query.h" && test -r "$EXT_DIR/libpg_query/libpg_query.a"; then
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

  dnl protobuf-c is bundled in libpg_query.a for static builds
  if test "$ext_shared" = "yes"; then
    PHP_ADD_LIBRARY(protobuf-c,, PG_QUERY_SHARED_LIBADD)
  fi

  PHP_SUBST(PG_QUERY_SHARED_LIBADD)

  dnl Define extension
  PHP_NEW_EXTENSION(pg_query, pg_query.c, $ext_shared,, -DZEND_ENABLE_STATIC_TSRMLS_CACHE=1)
fi
