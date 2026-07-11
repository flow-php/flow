dnl config.m4 for extension flow_php (Rust-based via ext-php-rs)

PHP_ARG_ENABLE([flow_php],
  [whether to enable flow_php support],
  [AS_HELP_STRING([--enable-flow_php],
    [Enable Flow PHP extension (requires Rust/cargo)])])

if test "$PHP_FLOW_PHP" != "no"; then
  AC_MSG_CHECKING([for cargo (Rust build tool)])
  AC_PATH_PROG(CARGO, cargo)
  if test -z "$CARGO"; then
    RUSTUP_CARGO=$(rustup which cargo 2>/dev/null)
    if test -n "$RUSTUP_CARGO"; then
      CARGO=$RUSTUP_CARGO
    fi
  fi
  if test -z "$CARGO"; then
    AC_MSG_ERROR([cargo is required to build the flow_php extension. Install Rust from https://rustup.rs/])
  fi
  AC_MSG_RESULT([$CARGO])

  AC_MSG_CHECKING([for rustc (Rust compiler)])
  AC_PATH_PROG(RUSTC, rustc)
  if test -z "$RUSTC"; then
    RUSTUP_RUSTC=$(rustup which rustc 2>/dev/null)
    if test -n "$RUSTUP_RUSTC"; then
      RUSTC=$RUSTUP_RUSTC
    fi
  fi
  if test -z "$RUSTC"; then
    AC_MSG_ERROR([rustc is required to build the flow_php extension. Install Rust from https://rustup.rs/])
  fi
  AC_MSG_RESULT([$RUSTC])

  dnl Create a dummy C file so phpize/configure infrastructure does not complain
  FLOW_PHP_EXT_DIR=$(pwd)
  if test ! -f "$FLOW_PHP_EXT_DIR/flow_php.c"; then
    echo "/* Dummy - actual extension built by Rust/cargo */" > "$FLOW_PHP_EXT_DIR/flow_php.c"
  fi

  PHP_NEW_EXTENSION(flow_php, flow_php.c, $ext_shared)
  PHP_ADD_MAKEFILE_FRAGMENT
fi
