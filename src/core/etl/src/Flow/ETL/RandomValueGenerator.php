<?php

declare(strict_types=1);

namespace Flow\ETL;

/**
 * Random value generator interface for producing random integers and strings.
 */
interface RandomValueGenerator
{
    public function int(int $min, int $max) : int;

    public function string(int $int) : string;
}
