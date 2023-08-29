<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class PregReplace implements Expression
{
    public function __construct(
        private readonly Expression $pattern,
        private readonly Expression $replacement,
        private readonly Expression $subject
    ) {
    }

    public function eval(Row $row) : ?string
    {
        /** @var array<array-key, non-empty-string>|non-empty-string $pattern */
        $pattern = $this->pattern->eval($row);
        /** @var mixed $replacement */
        $replacement = $this->replacement->eval($row);
        /** @var mixed $subject */
        $subject = $this->subject->eval($row);

        if (!\is_string($pattern) || !\is_string($replacement) || !\is_string($subject)) {
            return null;
        }

        return \preg_replace($pattern, $replacement, $subject);
    }
}
