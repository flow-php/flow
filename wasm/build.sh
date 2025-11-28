#!/usr/bin/env bash

# Build script for Flow PHP Interactive Playground
# Builds PHP 8.4.13

set -xeu

# Clear previous build log
rm -f build.log

# Log all output to build.log
exec > >(tee -a build.log) 2>&1
echo "=== Build started at $(date) ==="

PHP_VERSION=8.4.13
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
LIBPG_QUERY_VERSION=17-6.1.0
LIBPG_QUERY_DIR=libpg_query
LIBPG_QUERY_INSTALL_DIR="$PROJECT_ROOT/$LIBPG_QUERY_DIR"

if [ ! -d "$LIBPG_QUERY_DIR" ]; then
    git clone --depth=1 --branch=$LIBPG_QUERY_VERSION \
        https://github.com/pganalyze/libpg_query.git "$LIBPG_QUERY_DIR"
    cd $LIBPG_QUERY_DIR

    # Build with Emscripten - override CC and AR
    # The Makefile does AR := $(AR) rs, but command-line AR overrides this
    # So we must include the 'rs' flags ourselves. However, LLVM ar doesn't support 'g',
    # and the Makefile @ suppresses the echo, so we pass 'rcs' which is compatible.
    emmake make build -j$(nproc) CC=emcc AR="emar rcs"

    cd $PROJECT_ROOT
fi

echo "Download and extract PHP if needed"
if [ ! -d "$PHP_PATH" ]; then
    if [ ! -e $PHP_PATH.tar.xz ]; then
        wget https://www.php.net/distributions/php-$PHP_VERSION.tar.xz
    fi
    tar xf $PHP_PATH.tar.xz
fi

echo "Add pg_query extension"
PG_QUERY_EXT_SRC="$PROJECT_ROOT/../src/extension/pg-query-ext/ext"
PG_QUERY_EXT_DST="$PHP_PATH/ext/pg_query"

rm -rf "$PG_QUERY_EXT_DST"
cp -r "$PG_QUERY_EXT_SRC" "$PG_QUERY_EXT_DST"

echo "Configure PHP"

# Use -Oz for size optimization instead of -O3 for speed
export CFLAGS="-Oz -flto -fPIC -g0 -DZEND_MM_ERROR=0 -I$LIBXML2_INSTALL_DIR/include/libxml2 -I$LIBPG_QUERY_INSTALL_DIR -sUSE_ZLIB=1"
export LDFLAGS="-L$LIBXML2_INSTALL_DIR/lib -L$LIBPG_QUERY_INSTALL_DIR -sUSE_ZLIB=1"

cd $PHP_PATH

# Configure with extensions required by Flow PHP
# - bcmath: required by flow-php/parquet
# - libxml: required as base for XML extensions
# - xml, dom, xmlreader, xmlwriter: required by flow-php/etl-adapter-xml
# - phar, mbstring: essential PHP extensions
# - iconv: required by symfony/polyfill-mbstring

# Fix permissions for build scripts
chmod +x buildconf build/config-stubs build/shtool 2>/dev/null || true
find build -name "*.sh" -exec chmod +x {} \; 2>/dev/null || true

bash ./buildconf --force

set +e

emconfigure ./configure \
  --disable-all \
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
  --with-pg-query=$LIBPG_QUERY_INSTALL_DIR

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
  libs/libphp.a pib_eval.o $LIBXML2_INSTALL_DIR/lib/libxml2.a $LIBPG_QUERY_INSTALL_DIR/libpg_query.a -o out/php.js

echo "Copy outputs to web/landing/assets/wasm"
OUTPUT_DIR="$PROJECT_ROOT/../web/landing/assets/wasm"
mkdir -p "$OUTPUT_DIR"
cp out/php.wasm out/php.js "$OUTPUT_DIR/"

exit 0;
