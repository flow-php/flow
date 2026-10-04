<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_optional;
use function preg_match_all;

final class RegexAll implements ScalarFunction
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
        // PREG_OFFSET_CAPTURE and PREG_UNMATCHED_AS_NULL change each match's shape;
        // PREG_SET_ORDER transposes the whole result - all three change the column's type per flag value.
        if (
            ($flags & PREG_OFFSET_CAPTURE) !== 0
            || ($flags & PREG_UNMATCHED_AS_NULL) !== 0
            || ($flags & PREG_SET_ORDER) !== 0
        ) {
            throw new InvalidArgumentException(
                'RegexAll does not support PREG_OFFSET_CAPTURE, PREG_UNMATCHED_AS_NULL or PREG_SET_ORDER flags',
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $patterns = (new Parameter($this->pattern))->asStrings($rows, $context);
        $subjects = (new Parameter($this->subject))->asStrings($rows, $context);
        $offsets = (new Parameter($this->offset))->asInts($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($patterns as $i => $pattern) {
                $subject = $subjects[$i];
                $offset = $offsets[$i];

                if ($pattern === null) {
                    throw new InvalidArgumentException('RegexAll requires non-null pattern');
                }

                if ($subject === null) {
                    throw new InvalidArgumentException('RegexAll requires non-null subject');
                }

                if ($offset === null) {
                    throw new InvalidArgumentException('RegexAll requires non-null offset');
                }

                $matches = [];

                // Returns the number of full pattern matches (which might be zero), or false on failure.
                if (preg_match_all($pattern, $subject, $matches, $this->flags, $offset) !== false) {
                    if ($matches === [[]]) {
                        $results[] = null;

                        continue;
                    }

                    $results[] = $matches;

                    continue;
                }

                $results[] = null;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
