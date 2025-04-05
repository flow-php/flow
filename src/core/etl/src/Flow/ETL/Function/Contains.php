<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{type_array, type_string};
use Flow\ETL\Row;

final class Contains extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $haystack,
        private readonly ScalarFunction|string $needle,
    ) {
    }

    public function eval(Row $row) : bool
    {
        $haystack = (new Parameter($this->haystack))->as($row, type_string(), type_array());
        $needle = (new Parameter($this->needle))->asString($row);

        if ($haystack === null || $needle === null) {
            return false;
        }

        if (\is_string($haystack)) {
            return \str_contains($haystack, $needle);
        }

        if (\is_array($haystack)) {
            return \in_array($needle, $haystack, true);
        }

        return false;
    }
}
