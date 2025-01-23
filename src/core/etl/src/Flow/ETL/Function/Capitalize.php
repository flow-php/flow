<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\type_string;
use Flow\ETL\Function\ScalarFunction\TypedScalarFunction;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row;

final class Capitalize extends ScalarFunctionChain implements TypedScalarFunction
{
    public function __construct(private readonly ScalarFunction|string $string)
    {
    }

    public function eval(Row $row) : mixed
    {
        $string = (new Parameter($this->string))->eval($row);

        if ($string === null) {
            return null;
        }

        if (\function_exists('mb_convert_case')) {
            return \mb_convert_case((string) $string, \MB_CASE_TITLE);
        }

        return \ucwords((string) $string);
    }

    public function returns() : Type
    {
        return type_string();
    }
}
