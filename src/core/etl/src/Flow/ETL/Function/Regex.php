<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function preg_match;
use Flow\ETL\Row;

final class Regex extends ScalarFunctionChain
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

    /**
     * @return null|array<array-key, mixed>
     */
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
