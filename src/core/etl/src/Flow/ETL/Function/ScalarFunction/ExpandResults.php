<?php

declare(strict_types=1);

namespace Flow\ETL\Function\ScalarFunction;

interface ExpandResults
{
    public function expand() : bool;
}
