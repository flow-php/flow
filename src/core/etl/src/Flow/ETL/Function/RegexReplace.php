<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\{FlowContext, Row};

final class RegexReplace extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $pattern,
        private readonly ScalarFunction|string $replacement,
        private readonly ScalarFunction|string $subject,
        private readonly ScalarFunction|int|null $limit = null,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?string
    {
        $pattern = (new Parameter($this->pattern))->asString($row, $context);
        $replacement = (new Parameter($this->replacement))->asString($row, $context);
        $subject = (new Parameter($this->subject))->asString($row, $context);
        $limit = $this->limit ? (new Parameter($this->limit))->asInt($row, $context) : -1;

        if ($pattern === null || $replacement === null || $subject === null || $limit === null) {
            return null;
        }

        return \preg_replace($pattern, $replacement, $subject, $limit);
    }
}
