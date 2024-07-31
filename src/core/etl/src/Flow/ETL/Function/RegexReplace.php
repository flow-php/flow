<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class RegexReplace extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $pattern,
        private readonly ScalarFunction $replacement,
        private readonly ScalarFunction $subject,
        private readonly ?ScalarFunction $limit = null,
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

        $limit = $this->limit ? $this->limit->eval($row) : -1;

        if (!\is_string($pattern) || !\is_string($replacement) || !\is_string($subject) || !\is_int($limit)) {
            return null;
        }

        return \preg_replace($pattern, $replacement, $subject, $limit);
    }
}
