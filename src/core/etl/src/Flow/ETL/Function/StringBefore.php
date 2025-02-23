<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{type_list, type_string};
use function Symfony\Component\String\u;
use Flow\ETL\Function\ScalarFunction\TypedScalarFunction;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row;

final class StringBefore extends ScalarFunctionChain implements TypedScalarFunction
{
    public function __construct(
        private readonly ScalarFunction|string $string,
        private readonly ScalarFunction|string $needle,
        private readonly ScalarFunction|bool $includeNeedle = false,
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $string = (new Parameter($this->string))->asString($row);

        if ($string === null) {
            return null;
        }

        $needle = (new Parameter($this->needle))->as($row, type_string(), type_list(type_string()));
        $includeNeedle = (new Parameter($this->includeNeedle))->asBoolean($row);

        return u($string)->before($needle, $includeNeedle)->toString();
    }

    public function returns() : Type
    {
        return type_string();
    }
}
