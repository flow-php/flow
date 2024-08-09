<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Regex extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $pattern,
        private readonly ScalarFunction|string|array $subject,
        private readonly ScalarFunction|int $flags = 0,
        private readonly ScalarFunction|int $offset = 0,
    ) {
    }

    public function eval(Row $row) : ?array
    {
        $pattern = (new Parameter($this->pattern))->asString($row);
        $subject = (new Parameter($this->subject))->asString($row);
        $flags = (new Parameter($this->flags))->asInt($row);
        $offset = (new Parameter($this->offset))->asInt($row);

        if ($pattern === null || $subject === null || $flags === null || $offset === null) {
            return null;
        }

        // preg_match() returns 1 if the pattern matches given subject, 0 if it does not, or false on failure.
        /* @phpstan-ignore-next-line */
        if (\preg_match($pattern, $subject, $matches, $flags, $offset) === 1) {
            return $matches;
        }

        return null;
    }
}
