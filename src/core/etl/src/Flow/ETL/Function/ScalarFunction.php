<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

interface ScalarFunction extends FunctionChain
{
    public function eval(Row $row) : mixed;
}
