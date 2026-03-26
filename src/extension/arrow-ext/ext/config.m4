dnl config.m4 for extension arrow (Rust-based via ext-php-rs)

PHP_ARG_ENABLE([arrow],
  [whether to enable arrow support],
  [AS_HELP_STRING([--enable-arrow],
    [Enable Apache Arrow extension (requires Rust/cargo)])])

if test "$PHP_ARROW" != "no"; then
  AC_MSG_CHECKING([for cargo (Rust build tool)])
  AC_PATH_PROG(CARGO, cargo)
  if test -z "$CARGO"; then
    RUSTUP_CARGO=$(rustup which cargo 2>/dev/null)
    if test -n "$RUSTUP_CARGO"; then
      CARGO=$RUSTUP_CARGO
    fi
  fi
  if test -z "$CARGO"; then
    AC_MSG_ERROR([cargo is required to build the arrow extension. Install Rust from https://rustup.rs/])
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
    AC_MSG_ERROR([rustc is required to build the arrow extension. Install Rust from https://rustup.rs/])
  fi
  AC_MSG_RESULT([$RUSTC])

  dnl Create a dummy C file so phpize/configure infrastructure does not complain
  ARROW_EXT_DIR=$(pwd)
  if test ! -f "$ARROW_EXT_DIR/arrow.c"; then
    echo "/* Dummy - actual extension built by Rust/cargo */" > "$ARROW_EXT_DIR/arrow.c"
  fi

  PHP_NEW_EXTENSION(arrow, arrow.c, $ext_shared)
  PHP_ADD_MAKEFILE_FRAGMENT
fi
