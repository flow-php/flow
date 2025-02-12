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
        private readonly ScalarFunction|string $indexSearchString,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $string = (new Parameter($this->string))->asString($row);
        $indexSearchString = (new Parameter($this->indexSearchString))->asString($row);

        if ($string === null || $indexSearchString === null) {
            return null;
        }

        return u($string)->indexOf($this->indexSearchString);
    }

    public function returns() : Type
    {
        return type_int();
    }
}
