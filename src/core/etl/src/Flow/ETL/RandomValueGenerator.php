<?php

declare(strict_types=1);

namespace Flow\ETL;

interface RandomValueGenerator
{
    public function int(int $min, int $max) : int;

    public function string(int $int) : string;
}
