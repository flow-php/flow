<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class StartsWith implements Expression
{
    public function __construct(
        private readonly Expression $haystack,
        private readonly Expression $needle
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
