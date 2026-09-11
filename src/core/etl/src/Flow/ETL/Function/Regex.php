<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_optional;
use function preg_match;

final class Regex implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $pattern;
    private readonly ScalarFunction $subject;
    private readonly ScalarFunction $offset;

    /**
     * @param array<array-key, mixed>|ScalarFunction|string $subject
     */
    public function __construct(
        ScalarFunction|string $pattern,
        ScalarFunction|string|array $subject,
        private readonly int $flags = 0,
        ScalarFunction|int $offset = 0,
    ) {
        // PREG_OFFSET_CAPTURE changes each match from string to [string, int]; PREG_UNMATCHED_AS_NULL
        // makes elements nullable - both change the column's shape per flag value.
        if (($flags & PREG_OFFSET_CAPTURE) !== 0 || ($flags & PREG_UNMATCHED_AS_NULL) !== 0) {
            throw new InvalidArgumentException(
                'Regex does not support PREG_OFFSET_CAPTURE or PREG_UNMATCHED_AS_NULL flags',
            );
        }

        $this->pattern = $pattern instanceof ScalarFunction ? $pattern : lit($pattern);
        $this->subject = $subject instanceof ScalarFunction ? $subject : lit($subject);
        $this->offset = $offset instanceof ScalarFunction ? $offset : lit($offset);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->pattern, $this->subject, $this->offset];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $this->flags, $children[2]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_array());
    }

    /**
     * @return null|array<array-key, mixed>
     */
    public function eval(Row $row, FlowContext $context): ?array
    {
        $pattern = (new Parameter($this->pattern))->asString($row, $context);
        $subject = (new Parameter($this->subject))->asString($row, $context);
        $offset = (new Parameter($this->offset))->asInt($row, $context);

        if ($pattern === null) {
            throw new InvalidArgumentException('Regex requires non-null pattern');
        }

        if ($subject === null) {
            throw new InvalidArgumentException('Regex requires non-null subject');
        }

        if ($offset === null) {
            throw new InvalidArgumentException('Regex requires non-null offset');
        }

        $matches = [];

        // preg_match() returns 1 if the pattern matches given subject, 0 if it does not, or false on failure.
        if (preg_match($pattern, $subject, $matches, $this->flags, $offset) === 1) {
            return $matches;
        }

        return null;
    }
}
