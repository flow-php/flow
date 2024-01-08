<?php

declare(strict_types=1);

namespace Flow\ETL\Function\ScalarFunction;

interface UnpackResults
{
    public function unpackResults() : bool;
}
