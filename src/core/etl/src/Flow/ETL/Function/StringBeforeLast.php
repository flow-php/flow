<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\{type_list, type_string, type_union};
use function Symfony\Component\String\u;
use Flow\ETL\Row;

final class StringBeforeLast extends ScalarFunctionChain
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
        $typedNeedle = type_union(type_string(), type_list(type_string()))->assert($needle);
        $includeNeedle = (new Parameter($this->includeNeedle))->asBoolean($row);

        return u($string)->beforeLast($typedNeedle, $includeNeedle)->toString();
    }
}
