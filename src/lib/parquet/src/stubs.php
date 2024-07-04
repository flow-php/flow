<?php

declare(strict_types=1);

if (!\function_exists('lz4_compress')) {
    function lz4_compress(string $data, int $level = 0, ?string $extra = null) : string
    {
        throw new RuntimeException('The lz4 extension is not available');
    }
}

if (!\function_exists('lz4_uncompress')) {
    function lz4_uncompress(string $data, int $maxsize = -1, int $offset = -1) : string
    {
        throw new RuntimeException('The lz4 extension is not available');
    }
}

if (!\function_exists('zstd_compress')) {
    function zstd_compress(string $data, int $level = 3) : string
    {
        throw new RuntimeException('The Zstd extension is not available');
    }
}

if (!\function_exists('zstd_uncompress')) {
    function zstd_uncompress(string $data) : string
    {
        throw new RuntimeException('The Zstd extension is not available');
    }
}
