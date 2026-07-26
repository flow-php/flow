#!/usr/bin/env bash

# Build script for Flow PHP Interactive Playground
# Builds PHP 8.5.8

set -xeu

# Clear previous build log
rm -f build.log

# Log all output to build.log
exec > >(tee -a build.log) 2>&1
echo "=== Build started at $(date) ==="

PHP_VERSION=8.5.8
PHP_PATH=php-$PHP_VERSION

echo "Build libxml2 for WebAssembly"
LIBXML2_VERSION=2.11.4
LIBXML2_DIR=libxml2-$LIBXML2_VERSION
PROJECT_ROOT=$(pwd)
LIBXML2_INSTALL_DIR="$PROJECT_ROOT/$LIBXML2_DIR/install"

if [ ! -d "$LIBXML2_DIR" ]; then
    if [ ! -e $LIBXML2_DIR.tar.xz ]; then
        wget https://download.gnome.org/sources/libxml2/2.11/libxml2-$LIBXML2_VERSION.tar.xz
    fi
    tar xf $LIBXML2_DIR.tar.xz
    cd $LIBXML2_DIR

    # Configure libxml2 for WebAssembly
    emconfigure ./configure \
        --prefix=$LIBXML2_INSTALL_DIR \
        --disable-shared \
        --enable-static \
        --without-python \
        --without-threads \
        --without-history \
        --without-readline \
        --without-zlib \
        --without-lzma

    # Build libxml2
    emmake make -j$(nproc)
    emmake make install

    cd $PROJECT_ROOT
fi

echo "Build libpg_query for WebAssembly"
# Single source of truth is the pg_query extension's own pin, so the playground and the CLI can
# never disagree about which PostgreSQL grammar parses. build.sh already consumes that directory's
# ext/ sources below, so this adds no coupling that did not already exist.
LIBPG_QUERY_VERSION=$(sed -n 's/^LIBPG_QUERY_VERSION := //p' \
    "$PROJECT_ROOT/../src/extension/pg-query-ext/Makefile")

if [ -z "$LIBPG_QUERY_VERSION" ]; then
    echo "ERROR: could not read LIBPG_QUERY_VERSION from src/extension/pg-query-ext/Makefile"
    exit 1
fi

# Version in the directory name, like libxml2/libzip/php above: a bumped pin then misses the
# guard below and re-clones, instead of silently reusing the previous version's checkout.
# Note this tracks a moving branch, so an upstream branch move still needs a manual rm -rf.
LIBPG_QUERY_DIR=libpg_query-$LIBPG_QUERY_VERSION
LIBPG_QUERY_INSTALL_DIR="$PROJECT_ROOT/$LIBPG_QUERY_DIR"

# Guard on the built archive rather than the directory, like libzip below: a build that fails
# part way then leaves a tree that a directory check would skip, and the missing archive would
# only surface much later at link time.
if [ ! -f "$LIBPG_QUERY_INSTALL_DIR/libpg_query.a" ]; then
    rm -rf "$LIBPG_QUERY_DIR"
    git clone --depth=1 --branch=$LIBPG_QUERY_VERSION \
        https://github.com/pganalyze/libpg_query.git "$LIBPG_QUERY_DIR"
    cd $LIBPG_QUERY_DIR

    # Build with Emscripten - override CC, AR and ARFLAGS.
    # libpg_query 18 keeps AR and ARFLAGS separate (Makefile:71-72), unlike 17's combined
    # AR := $(AR) rs, so folding the flags into AR gives `emar rcs rs` -- two operations, which
    # llvm-ar rejects. Both have to come from the command line: the Makefile's `?=` never fires,
    # because GNU make predefines ARFLAGS=-rv and `?=` only assigns when the origin is `undefined`.
    # `rcs` rather than the default `-rv` so the archive still gets its symbol index.
    emmake make build -j$(nproc) CC=emcc AR=emar ARFLAGS=rcs

    cd $PROJECT_ROOT
fi

echo "Build libzip for WebAssembly"
LIBZIP_VERSION=1.11.3
LIBZIP_DIR=libzip-$LIBZIP_VERSION
LIBZIP_INSTALL_DIR="$PROJECT_ROOT/$LIBZIP_DIR/install"

# Check for installed library, not just source directory
if [ ! -f "$LIBZIP_INSTALL_DIR/lib/libzip.a" ]; then
    # First, ensure Emscripten's zlib port is built by triggering a compile
    # This downloads and builds zlib to the Emscripten cache
    echo "int main(){return 0;}" > /tmp/zlib_test.c
    # -flto must match CFLAGS below: emcc caches a separate port build per variant, and this
    # script links the lto/ one. Without it only the non-LTO libz.a is produced and the check
    # after this block fails.
    emcc -flto -sUSE_ZLIB=1 /tmp/zlib_test.c -o /tmp/zlib_test.js 2>/dev/null || true
    rm -f /tmp/zlib_test.c /tmp/zlib_test.js /tmp/zlib_test.wasm

    # Get Emscripten cache path and locate zlib
    EM_CACHE=$(em-config CACHE)
    ZLIB_LIBRARY="$EM_CACHE/sysroot/lib/wasm32-emscripten/lto/libz.a"
    ZLIB_INCLUDE_DIR="$EM_CACHE/sysroot/include"

    echo "Using zlib from Emscripten cache:"
    echo "  ZLIB_LIBRARY=$ZLIB_LIBRARY"
    echo "  ZLIB_INCLUDE_DIR=$ZLIB_INCLUDE_DIR"

    if [ ! -f "$ZLIB_LIBRARY" ]; then
        echo "ERROR: zlib library not found at $ZLIB_LIBRARY"
        exit 1
    fi

    if [ ! -e $LIBZIP_DIR.tar.xz ]; then
        wget https://libzip.org/download/libzip-$LIBZIP_VERSION.tar.xz
    fi
    tar xf $LIBZIP_DIR.tar.xz
    cd $LIBZIP_DIR

    mkdir -p build && cd build

    # Configure libzip for WebAssembly using CMake
    # Provide explicit paths to Emscripten's zlib (from its ports system)
    # Disable encryption and optional compression to minimize dependencies
    emcmake cmake .. \
        -DCMAKE_INSTALL_PREFIX=$LIBZIP_INSTALL_DIR \
        -DZLIB_LIBRARY=$ZLIB_LIBRARY \
        -DZLIB_INCLUDE_DIR=$ZLIB_INCLUDE_DIR \
        -DBUILD_SHARED_LIBS=OFF \
        -DENABLE_COMMONCRYPTO=OFF \
        -DENABLE_GNUTLS=OFF \
        -DENABLE_MBEDTLS=OFF \
        -DENABLE_OPENSSL=OFF \
        -DENABLE_WINDOWS_CRYPTO=OFF \
        -DENABLE_BZIP2=OFF \
        -DENABLE_LZMA=OFF \
        -DENABLE_ZSTD=OFF \
        -DBUILD_TOOLS=OFF \
        -DBUILD_REGRESS=OFF \
        -DBUILD_EXAMPLES=OFF \
        -DBUILD_DOC=OFF

    emmake make -j$(nproc)
    emmake make install

    cd $PROJECT_ROOT
fi

echo "Download and extract PHP if needed"
if [ ! -d "$PHP_PATH" ]; then
    if [ ! -e $PHP_PATH.tar.xz ]; then
        wget https://www.php.net/distributions/php-$PHP_VERSION.tar.xz
    fi
    tar xf $PHP_PATH.tar.xz
fi

echo "Patch php-src"
# Re-runs reuse an already extracted $PHP_PATH, so this has to be idempotent: a cleanly applying
# reverse patch means it is already in place. Anything else is a real failure and set -e ends here.
PHP_PATCH="$PROJECT_ROOT/patches/php-8.5-opcache-unistd.patch"
cd "$PHP_PATH"

if patch -p1 --reverse --dry-run --force --silent <"$PHP_PATCH" >/dev/null 2>&1; then
    echo "  already applied: $(basename "$PHP_PATCH")"
else
    patch -p1 --forward <"$PHP_PATCH"
fi

cd "$PROJECT_ROOT"

echo "Add pg_query extension"
PG_QUERY_EXT_SRC="$PROJECT_ROOT/../src/extension/pg-query-ext/ext"
PG_QUERY_EXT_DST="$PHP_PATH/ext/pg_query"

rm -rf "$PG_QUERY_EXT_DST"
cp -r "$PG_QUERY_EXT_SRC" "$PG_QUERY_EXT_DST"

echo "Add snappy extension"
SNAPPY_EXT_DIR=php-ext-snappy
if [ ! -d "$SNAPPY_EXT_DIR" ]; then
    git clone --recursive --depth=1 https://github.com/kjdev/php-ext-snappy.git "$SNAPPY_EXT_DIR"
fi

SNAPPY_EXT_DST="$PHP_PATH/ext/snappy"
rm -rf "$SNAPPY_EXT_DST"
cp -r "$SNAPPY_EXT_DIR" "$SNAPPY_EXT_DST"

echo "Configure PHP"

# Use -Oz for size optimization instead of -O3 for speed
# -DHAVE_REALLOCARRAY=1: emcc's link probe for reallocarray() fails while its sysroot header still
# declares the symbol, so main/php_glob.c (always built since 8.5) compiles its own static copy and
# clashes. See https://github.com/php/php-src/issues/19152.
export CFLAGS="-Oz -flto -fPIC -g0 -DZEND_MM_ERROR=0 -DHAVE_REALLOCARRAY=1 -I$LIBXML2_INSTALL_DIR/include/libxml2 -I$LIBPG_QUERY_INSTALL_DIR -I$LIBPG_QUERY_INSTALL_DIR/src -I$LIBZIP_INSTALL_DIR/include -sUSE_ZLIB=1"
export CXXFLAGS="-Oz -flto -fPIC -g0 -std=c++11 -sUSE_ZLIB=1"
export LDFLAGS="-L$LIBXML2_INSTALL_DIR/lib -L$LIBPG_QUERY_INSTALL_DIR -L$LIBZIP_INSTALL_DIR/lib -sUSE_ZLIB=1"

# Set PKG_CONFIG_PATH so PHP configure can find libzip
# Note: emconfigure overrides PKG_CONFIG_PATH with PKG_CONFIG_LIBDIR, so we also set LIBZIP_* directly
export PKG_CONFIG_PATH="$LIBZIP_INSTALL_DIR/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
export LIBZIP_CFLAGS="-I$LIBZIP_INSTALL_DIR/include"
export LIBZIP_LIBS="-L$LIBZIP_INSTALL_DIR/lib -lzip"

cd $PHP_PATH

# Configure with extensions required by Flow PHP
# - bcmath: required by flow-php/parquet
# - libxml: required as base for XML extensions
# - xml, dom, xmlreader, xmlwriter: required by flow-php/etl-adapter-xml
# - phar, mbstring: essential PHP extensions
# - iconv: required by symfony/polyfill-mbstring
# - zip: required by flow-php/etl-adapter-excel (XLSX files are ZIP archives)

# Fix permissions for build scripts
chmod +x buildconf build/config-stubs build/shtool 2>/dev/null || true
find build -name "*.sh" -exec chmod +x {} \; 2>/dev/null || true

bash ./buildconf --force

set +e

# --disable-opcache-jit is NOT redundant next to --disable-all. Since php-src 7b4c14dc1016 opcache
# is non-optional and --disable-all cannot reach it, but its JIT is still gated on $host_cpu -- and
# emconfigure passes no --host, so config.guess reports the BUILD machine. On any host autoconf
# calls JIT-capable (x86_64, aarch64, ...) emcc would be handed jit/ir/*.c and dynasm for the host
# ISA to compile into a wasm32 binary. Same class of workaround as --disable-fiber-asm below.
# --disable-huge-code-pages is pointless under wasm and matches the other PHP-to-wasm builds.
# Both pass [no] as their 5th PHP_ARG_ENABLE arg, so neither is affected by --disable-all.
emconfigure ./configure \
  --disable-all \
  --disable-opcache-jit \
  --disable-huge-code-pages \
  --disable-cgi \
  --disable-cli \
  --disable-rpath \
  --disable-phpdbg \
  --with-valgrind=no \
  --without-pear \
  --without-valgrind \
  --without-pcre-jit \
  --with-layout=GNU \
  --enable-bcmath \
  --enable-embed=static \
  --enable-phar \
  --enable-mbstring \
  --disable-mbregex \
  --disable-fiber-asm \
  --enable-filter \
  --enable-tokenizer \
  --with-zlib \
  --with-iconv \
  --with-libxml \
  --enable-xml \
  --enable-dom \
  --enable-xmlreader \
  --enable-xmlwriter \
  --enable-pg-query \
  --with-pg-query=$LIBPG_QUERY_INSTALL_DIR \
  --enable-snappy \
  --with-zip

if [ $? -ne 0 ]; then
    echo "emconfigure failed. Content of config.log:"
    cat config.log
    exit 1
fi

set -e

echo "Build PHP"

emmake make clean
# Use 75% of available cores
cores=$(nproc)
build_cores=$((cores * 3 / 4))
if [ $build_cores -lt 1 ]; then
    build_cores=1
fi
echo "Using $build_cores cores (of $cores available)"
emmake make -j$build_cores

rm -rf out
mkdir -p out

echo "Compile pib_eval wrapper"
emcc $CFLAGS -I . -I Zend -I main -I TSRM/ ../pib_eval.c -c -o pib_eval.o

echo "Link everything together"
emcc $CFLAGS $LDFLAGS \
  -s ENVIRONMENT=web \
  -s EXPORTED_FUNCTIONS='["_pib_eval", "_pib_force_exit", "_php_embed_init", "_zend_eval_string", "_php_embed_shutdown"]' \
  -s EXPORTED_RUNTIME_METHODS='["ccall","FS","UTF8ToString","lengthBytesUTF8","stringToUTF8","getValue","setValue","ENV"]' \
  -s MODULARIZE=1 \
  -s EXPORT_NAME="'PHP'" \
  -s INITIAL_MEMORY=134217728 \
  -s ALLOW_MEMORY_GROWTH=1 \
  -s MAXIMUM_MEMORY=2147483648 \
  -s TOTAL_STACK=33554432 \
  -s STACK_SIZE=5242880 \
  -s ASSERTIONS=0 \
  -s INVOKE_RUN=0 \
  -s ERROR_ON_UNDEFINED_SYMBOLS=0 \
  -s ASYNCIFY=1 \
  -s STACK_OVERFLOW_CHECK=0 \
  -s SAFE_HEAP=0 \
  libs/libphp.a pib_eval.o $LIBXML2_INSTALL_DIR/lib/libxml2.a $LIBPG_QUERY_INSTALL_DIR/libpg_query.a $LIBZIP_INSTALL_DIR/lib/libzip.a -o out/php.js

echo "Copy outputs to web/landing/assets/wasm"
OUTPUT_DIR="$PROJECT_ROOT/../web/landing/assets/wasm"
mkdir -p "$OUTPUT_DIR"
cp out/php.wasm out/php.js "$OUTPUT_DIR/"

exit 0;
