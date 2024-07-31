<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Regex extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $pattern,
        private readonly ScalarFunction $subject,
        private readonly ?ScalarFunction $flags = null,
        private readonly ?ScalarFunction $offset = null,
    ) {
    }

    public function eval(Row $row) : ?array
    {
        /** @var array<array-key, non-empty-string>|non-empty-string $pattern */
        $pattern = $this->pattern->eval($row);
        $subject = $this->subject->eval($row);
        $flags = $this->flags ? $this->flags->eval($row) : 0;
        $offset = $this->offset ? $this->offset->eval($row) : 0;

        if (!\is_string($pattern) || !\is_string($subject) || !\is_int($flags) || !\is_int($offset)) {
            return null;
        }

        // preg_match() returns 1 if the pattern matches given subject, 0 if it does not, or false on failure.
        if (\preg_match($pattern, $subject, $matches) === 1) {
            return $matches;
        }

        return null;
    }
}
