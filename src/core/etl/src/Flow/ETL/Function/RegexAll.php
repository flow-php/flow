<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class RegexAll extends ScalarFunctionChain
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
    public function eval(Row $row, FlowContext $context) : ?array
    {
        $pattern = (new Parameter($this->pattern))->asString($row, $context);
        $subject = (new Parameter($this->subject))->asString($row, $context);
        $flags = (new Parameter($this->flags))->asInt($row, $context);
        $offset = (new Parameter($this->offset))->asInt($row, $context);

        if ($pattern === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('RegexAll requires non-null pattern'));
        }

        if ($subject === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('RegexAll requires non-null subject'));
        }

        if ($flags === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('RegexAll requires non-null flags'));
        }

        if ($offset === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('RegexAll requires non-null offset'));
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
