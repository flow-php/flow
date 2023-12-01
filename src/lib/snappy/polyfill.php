<?php

declare(strict_types=1);

use Flow\Snappy\Snappy;

if (!\function_exists('snappy_compress')) {
    function snappy_compress(string $plainText) : string
    {
        return (new Snappy())->compress($plainText);
    }
}

if (!\function_exists('snappy_uncompress')) {
    function snappy_uncompress(string $compressedText) : string
    {
        return (new Snappy())->uncompress($compressedText);
    }
}

