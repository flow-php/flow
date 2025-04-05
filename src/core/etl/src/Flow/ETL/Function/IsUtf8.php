<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\{b};
use Flow\ETL\Row;

final class IsUtf8 extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction|string $string)
    {
    }

    public function eval(Row $row) : bool
    {
        $string = (new Parameter($this->string))->asString($row);

        if ($string === null) {
            return false;
        }

        return b($string)->isUtf8();
    }
}
