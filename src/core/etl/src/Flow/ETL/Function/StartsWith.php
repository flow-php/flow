<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class StartsWith implements ScalarFunction
{
    public function __construct(
        private readonly ScalarFunction $haystack,
        private readonly ScalarFunction $needle
    ) {
    }

    public function eval(Row $row) : bool
    {
        $haystack = $this->haystack->eval($row);
        $needle = $this->needle->eval($row);

        if (!\is_string($needle) || !\is_string($haystack)) {
            return false;
        }

        return \str_starts_with($haystack, $needle);
    }
}
