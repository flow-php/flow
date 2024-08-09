<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class RegexAll extends ScalarFunctionChain
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

        // Returns the number of full pattern matches (which might be zero), or false on failure.
        if (\preg_match_all($pattern, $subject, $matches, $flags, $offset) !== false) {
            if ($matches === [[]]) {
                return null;
            }

            return $matches;
        }

        return null;
    }
}
