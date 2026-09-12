<?php

declare(strict_types=1);

namespace Flow\Snappy;

use function array_values;
use function pack;
use function unpack;

final class Snappy
{
    public function __construct() {}

    public function compress(string $plainText): string
    {
        if ($plainText === '') {
            return $plainText;
        }

        $byteArray = array_values(unpack('C*', $plainText) ?: []);

        $outputBuffer = [];
        (new SnappyCompressor($byteArray))->compressToBuffer($outputBuffer);

        return pack('C*', ...$outputBuffer);
    }

    public function uncompress(string $compressedText): string|false
    {
        if ($compressedText === '') {
            return $compressedText;
        }

        $byteArray = array_values(unpack('C*', $compressedText) ?: []);

        $outputBuffer = [];
        if (!(new SnappyDecompressor($byteArray))->uncompressToBuffer($outputBuffer)) {
            return false;
        }

        return pack('C*', ...$outputBuffer);
    }
}
