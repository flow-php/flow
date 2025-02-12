<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{type_int};
use function Symfony\Component\String\u;
use Flow\ETL\Function\ScalarFunction\TypedScalarFunction;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row;

final class IndexOf extends ScalarFunctionChain implements TypedScalarFunction
{
    public function __construct(
        private readonly ScalarFunction|string $string,
        private readonly ScalarFunction|string $needle,
        private readonly int $offset = 0,
    ) {
    }

    public function eval(Row $row) : int|false|null
    {
        $string = (new Parameter($this->string))->asString($row);
        $needle = (new Parameter($this->needle))->asString($row);

        if ($string === null || $needle === null) {
            return false;
        }

        return u($string)->indexOf($needle, $this->offset);
    }

    public function returns() : Type
    {
        return type_int();
    }
}
