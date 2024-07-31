<?php

declare(strict_types=1);

namespace Flow\ETL;

final class NativePHPRandomValueGenerator implements RandomValueGenerator
{
    public function int(int $min, int $max) : int
    {
        return \random_int($min, $max);
    }

    public function string(int $int) : string
    {
        $bytes = (int) \ceil($int / 2);
        $bytes >= 1 ?: $bytes = 1;

        return \substr(\bin2hex(\random_bytes($bytes)), 0, \max(0, $int));
    }
}
