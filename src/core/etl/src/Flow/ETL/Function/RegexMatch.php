<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\{FlowContext, Row};

final class RegexMatch extends ScalarFunctionChain
{
    /**
     * @param ScalarFunction|string $pattern
     * @param array<array-key, mixed>|ScalarFunction|string $subject
     * @param int|ScalarFunction $flags
     * @param int|ScalarFunction $offset
     */
    public function __construct(
        private readonly ScalarFunction|string $pattern,
        private readonly ScalarFunction|string|array $subject,
        private readonly ScalarFunction|int $flags = 0,
        private readonly ScalarFunction|int $offset = 0,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?bool
    {
        $pattern = (new Parameter($this->pattern))->asString($row, $context);
        $subject = (new Parameter($this->subject))->asString($row, $context);
        $flags = (new Parameter($this->flags))->asInt($row, $context);
        $offset = (new Parameter($this->offset))->asInt($row, $context);

        if ($pattern === null || $subject === null || $flags === null || $offset === null) {
            return null;
        }

        /** @phpstan-ignore-next-line */
        return \preg_match(pattern: $pattern, subject: $subject, flags: $flags, offset: $offset) === 1;
    }
}
