<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class PregMatchAll implements Expression
{
    public function __construct(
        private readonly Expression $pattern,
        private readonly Expression $subject,
        private readonly ?Expression $flags = null
    ) {
    }

    public function eval(Row $row) : array
    {
        /** @var array<array-key, non-empty-string>|non-empty-string $pattern */
        $pattern = $this->pattern->eval($row);
        $subject = $this->subject->eval($row);
        $flags = $this->flags ? $this->flags->eval($row) : 0;

        if (!\is_string($pattern) || !\is_string($subject) || !\is_int($flags)) {
            return [];
        }

        $matches = [];
        \preg_match_all($pattern, $subject, $matches, $flags);

        return $matches;
    }
}
