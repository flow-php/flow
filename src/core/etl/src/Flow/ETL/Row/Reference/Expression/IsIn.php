<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class IsIn implements Expression
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

        if (!\is_array($haystack)) {
            return false;
        }

        return \in_array($needle, $haystack, true);
    }
}
