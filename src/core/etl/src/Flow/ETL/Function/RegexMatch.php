<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function preg_match;

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
    ) {}

    public function eval(Row $row, FlowContext $context): ?bool
    {
        $pattern = (new Parameter($this->pattern))->asString($row, $context);
        $subject = (new Parameter($this->subject))->asString($row, $context);
        $flags = (new Parameter($this->flags))->asInt($row, $context);
        $offset = (new Parameter($this->offset))->asInt($row, $context);

        if ($pattern === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('RegexMatch requires non-null pattern'));
        }

        if ($subject === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('RegexMatch requires non-null subject'));
        }

        if ($flags === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('RegexMatch requires non-null flags'));
        }

        if ($offset === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('RegexMatch requires non-null offset'));
        }

        /** @phpstan-ignore argument.type */
        return preg_match(pattern: $pattern, subject: $subject, flags: $flags, offset: $offset) === 1;
    }
}
