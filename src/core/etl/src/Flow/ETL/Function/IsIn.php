<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class IsIn extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|array $haystack,
        private readonly mixed $needle,
    ) {
    }

    public function eval(Row $row) : bool
    {
        $haystack = (new Parameter($this->haystack))->asArray($row);
        $needle = (new Parameter($this->needle))->eval($row);

        if ($haystack === null) {
            return false;
        }

        return \in_array($needle, $haystack, true);
    }
}
