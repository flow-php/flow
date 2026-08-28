<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;
use function preg_match_all;

final class RegexMatchAll implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @param ScalarFunction|string $pattern
     * @param array<array-key, mixed>|ScalarFunction|string $subject
     * @param int|ScalarFunction $flags
     * @param int|ScalarFunction $offset
     */
    private readonly ScalarFunction $pattern;
    private readonly ScalarFunction $subject;
    private readonly ScalarFunction $flags;
    private readonly ScalarFunction $offset;

    public function __construct(
        ScalarFunction|string $pattern,
        ScalarFunction|string|array $subject,
        ScalarFunction|int $flags = 0,
        ScalarFunction|int $offset = 0,
    ) {
        $this->pattern = $pattern instanceof ScalarFunction ? $pattern : lit($pattern);
        $this->subject = $subject instanceof ScalarFunction ? $subject : lit($subject);
        $this->flags = $flags instanceof ScalarFunction ? $flags : lit($flags);
        $this->offset = $offset instanceof ScalarFunction ? $offset : lit($offset);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->pattern, $this->subject, $this->flags, $this->offset];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2], $children[3]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return (new Nullability())->any(
            type_boolean(),
            $this->pattern->returns(),
            $this->subject->returns(),
            $this->flags->returns(),
            $this->offset->returns(),
        );
    }

    public function eval(Row $row, FlowContext $context): ?bool
    {
        $pattern = (new Parameter($this->pattern))->asString($row, $context);
        $subject = (new Parameter($this->subject))->asString($row, $context);
        $flags = (new Parameter($this->flags))->asInt($row, $context);
        $offset = (new Parameter($this->offset))->asInt($row, $context);

        if ($pattern === null) {
            throw new InvalidArgumentException('RegexMatchAll requires non-null pattern');
        }

        if ($subject === null) {
            throw new InvalidArgumentException('RegexMatchAll requires non-null subject');
        }

        if ($flags === null) {
            throw new InvalidArgumentException('RegexMatchAll requires non-null flags');
        }

        if ($offset === null) {
            throw new InvalidArgumentException('RegexMatchAll requires non-null offset');
        }

        return preg_match_all(pattern: $pattern, subject: $subject, flags: $flags, offset: $offset) !== false;
    }
}
