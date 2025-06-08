<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function\Fixtures\CallUserFunc;

final class Calculator
{
    public function count(array $array) : int
    {
        return \count($array);
    }
}
