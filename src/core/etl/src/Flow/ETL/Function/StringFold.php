<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\u;
use Flow\ETL\Row;

final class StringFold extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction|string $string)
    {
    }

    public function eval(Row $row) : ?string
    {
        $string = (new Parameter($this->string))->asString($row);

        if ($string === null) {
            return null;
        }

        return u($string)->folded()->toString();
    }
}
