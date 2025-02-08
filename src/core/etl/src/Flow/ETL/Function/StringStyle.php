<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\type_string;
use Flow\ETL\Function\ScalarFunction\TypedScalarFunction;
use Flow\ETL\Function\StyleConverter\StringStyles;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row;

final class StringStyle extends ScalarFunctionChain implements TypedScalarFunction
{
    public function __construct(private readonly ScalarFunction|string $string, private readonly StringStyles $style)
    {
    }

    public function eval(Row $row) : mixed
    {
        $string = (new Parameter($this->string))->asString($row);

        if ($string === null) {
            return null;
        }

        return $this->style->convert($string);
    }

    public function returns() : Type
    {
        return type_string();
    }
}
