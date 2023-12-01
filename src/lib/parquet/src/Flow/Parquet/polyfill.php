<?php

declare(strict_types=1);

use Flow\ETL\Exception\RuntimeException;

if (!\function_exists('brotli_compress')) {
    function brotli_compress(string $plainText) : string|false
    {
        throw new RuntimeException('Brotli PHP extension is not installed, please follow instructions from: https://github.com/kjdev/php-ext-brotli');
    }
}

if (!\function_exists('brotli_uncompress')) {
    function brotli_uncompress(string $compressedText) : string|false
    {
        throw new RuntimeException('Brotli PHP extension is not installed, please follow instructions from: https://github.com/kjdev/php-ext-brotli');
    }
}

if (!\function_exists('lz4_compress')) {
    function lz4_compress(string $plainText) : string|false
    {
        throw new RuntimeException('LZ4 PHP extension is not installed, please follow instructions from: https://github.com/kjdev/php-ext-lz4');
    }
}

if (!\function_exists('lz4_uncompress')) {
    function lz4_uncompress(string $compressedText) : string|false
    {
        throw new RuntimeException('LZ4 PHP extension is not installed, please follow instructions from: https://github.com/kjdev/php-ext-lz4');
    }
}

if (!\function_exists('zstd_compress')) {
    function zstd_compress(string $plainText) : string|false
    {
        throw new RuntimeException('Zstd PHP extension is not installed, please follow instructions from: https://github.com/kjdev/php-ext-zstd');
    }
}

if (!\function_exists('zstd_uncompress')) {
    function zstd_uncompress(string $compressedText) : string|false
    {
        throw new RuntimeException('Zstd PHP extension is not installed, please follow instructions from: https://github.com/kjdev/php-ext-zstd');
    }
}
