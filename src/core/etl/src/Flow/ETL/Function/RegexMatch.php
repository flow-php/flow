<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class RegexMatch extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $pattern,
        private readonly ScalarFunction|string|array $subject,
        private readonly ScalarFunction|int $flags = 0,
        private readonly ScalarFunction|int $offset = 0,
    ) {
    }

    public function eval(Row $row) : ?bool
    {
        $pattern = (new Parameter($this->pattern))->asString($row);
        $subject = (new Parameter($this->subject))->asString($row);
        $flags = (new Parameter($this->flags))->asInt($row);
        $offset = (new Parameter($this->offset))->asInt($row);

        if ($pattern === null || $subject === null || $flags === null || $offset === null) {
            return null;
        }

        /** @phpstan-ignore-next-line */
        return \preg_match(pattern: $pattern, subject: $subject, flags: $flags, offset: $offset) === 1;
    }
}
