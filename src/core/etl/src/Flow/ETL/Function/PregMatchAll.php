<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class PregMatchAll extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $pattern,
        private readonly ScalarFunction $subject,
        private readonly ?ScalarFunction $flags = null
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
