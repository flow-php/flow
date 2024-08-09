<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class EndsWith extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $haystack,
        private readonly ScalarFunction|string $needle
    ) {
    }

    public function eval(Row $row) : bool
    {
        $haystack = (new Parameter($this->haystack))->asString($row);
        $needle = (new Parameter($this->needle))->asString($row);

        if ($haystack === null || $needle === null) {
            return false;
        }

        return \str_ends_with($haystack, $needle);
    }
}
