<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class RegexMatchAll extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $pattern,
        private readonly ScalarFunction $subject,
        private readonly ?ScalarFunction $flags = null,
        private readonly ?ScalarFunction $offset = null,
    ) {
    }

    public function eval(Row $row) : ?bool
    {
        /** @var non-empty-string $pattern */
        $pattern = $this->pattern->eval($row);
        $subject = $this->subject->eval($row);
        $flags = $this->flags ? $this->flags->eval($row) : 0;
        $offset = $this->offset ? $this->offset->eval($row) : 0;

        if (!\is_string($pattern) || !\is_string($subject) || !\is_int($flags) || !\is_int($offset)) {
            return null;
        }

        return \preg_match_all(pattern: $pattern, subject: $subject, flags: $flags, offset: $offset) !== false;
    }
}
