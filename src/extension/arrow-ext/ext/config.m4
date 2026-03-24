dnl config.m4 for extension arrow (Rust-based via ext-php-rs)
dnl This is a best-effort PIE compatibility shim delegating to cargo

PHP_ARG_ENABLE([arrow],
  [whether to enable arrow support],
  [AS_HELP_STRING([--enable-arrow],
    [Enable Apache Arrow extension (requires Rust/cargo)])])

if test "$PHP_ARROW" != "no"; then
  AC_MSG_CHECKING([for cargo (Rust build tool)])
  AC_PATH_PROG(CARGO, cargo, no)
  if test "$CARGO" = "no"; then
    AC_MSG_ERROR([cargo is required to build the arrow extension. Install Rust from https://rustup.rs/])
  fi
  AC_MSG_RESULT([$CARGO])

  dnl Build via cargo in the parent directory (where Cargo.toml lives)
  EXT_DIR=$(pwd)
  ARROW_SRC_DIR=$(dirname "$EXT_DIR")

  AC_MSG_NOTICE([Building arrow extension via cargo...])
  (cd "$ARROW_SRC_DIR" && $CARGO build --release) || AC_MSG_ERROR([cargo build failed])

  dnl Detect the built library
  case $host_os in
    darwin*)
      ARROW_LIB="$ARROW_SRC_DIR/target/release/libarrow.dylib"
      ;;
    *)
      ARROW_LIB="$ARROW_SRC_DIR/target/release/libarrow.so"
      ;;
  esac

  if test ! -f "$ARROW_LIB"; then
    AC_MSG_ERROR([Built library not found at $ARROW_LIB])
  fi

  dnl Copy built library to expected location
  mkdir -p "$EXT_DIR/modules"
  cp "$ARROW_LIB" "$EXT_DIR/modules/arrow.so"

  dnl Create a dummy C file so phpize/configure infrastructure does not complain
  if test ! -f "$EXT_DIR/arrow.c"; then
    echo "/* Dummy - actual extension built by Rust/cargo */" > "$EXT_DIR/arrow.c"
  fi

  PHP_NEW_EXTENSION(arrow, arrow.c, $ext_shared)
fi
