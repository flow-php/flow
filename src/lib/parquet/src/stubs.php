<?php

declare(strict_types=1);

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
