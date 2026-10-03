<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;
use function preg_match_all;

final class RegexMatchAll implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $pattern;
    private readonly ScalarFunction $subject;
    private readonly ScalarFunction $flags;
    private readonly ScalarFunction $offset;

    /**
     * @param array<array-key, mixed>|ScalarFunction|string $subject
     */
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $patterns = (new Parameter($this->pattern))->asStrings($rows, $context);
        $subjects = (new Parameter($this->subject))->asStrings($rows, $context);
        $flagsList = (new Parameter($this->flags))->asInts($rows, $context);
        $offsets = (new Parameter($this->offset))->asInts($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($patterns as $i => $pattern) {
                $subject = $subjects[$i];
                $flags = $flagsList[$i];
                $offset = $offsets[$i];

                if ($pattern === null || $subject === null || $flags === null || $offset === null) {
                    $results[] = null;

                    continue;
                }

                $results[] =
                    preg_match_all(pattern: $pattern, subject: $subject, flags: $flags, offset: $offset) !== false;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
