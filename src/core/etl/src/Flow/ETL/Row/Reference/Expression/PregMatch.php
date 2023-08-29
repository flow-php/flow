<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class PregMatch implements Expression
{
    public function __construct(
        private readonly Expression $pattern,
        private readonly Expression $subject
    ) {
    }

    public function eval(Row $row) : ?bool
    {
        /** @var array<array-key, non-empty-string>|non-empty-string $pattern */
        $pattern = $this->pattern->eval($row);
        $subject = $this->subject->eval($row);

        if (!\is_string($pattern) || !\is_string($subject)) {
            return null;
        }

        return \preg_match($pattern, $subject) === 1;
    }
}
