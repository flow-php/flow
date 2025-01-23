<?php

declare(strict_types=1);

namespace Flow\ETL\Function\ScalarFunction;

use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Row;

interface ExpandResults extends ScalarFunction
{
    public function eval(Row $row) : ?array;
}
