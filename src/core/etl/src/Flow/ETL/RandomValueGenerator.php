<?php

declare(strict_types=1);

namespace Flow\ETL;

interface RandomValueGenerator
{
    public static function int(int $min, int $max) : int;

    /** @param int<1, max> $int */
    public static function string(int $int) : string;
}
