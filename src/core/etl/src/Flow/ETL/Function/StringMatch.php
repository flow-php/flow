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

use function count;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_optional;
use function Symfony\Component\String\s;

final class StringMatch implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $haystack;
    private readonly ScalarFunction $pattern;

    public function __construct(ScalarFunction|string $haystack, ScalarFunction|string $pattern)
    {
        $this->haystack = $haystack instanceof ScalarFunction ? $haystack : lit($haystack);
        $this->pattern = $pattern instanceof ScalarFunction ? $pattern : lit($pattern);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->haystack, $this->pattern];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1]);
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
        $haystacks = (new Parameter($this->haystack))->asStrings($rows, $context);
        $patterns = (new Parameter($this->pattern))->asStrings($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($haystacks as $i => $haystack) {
                $pattern = $patterns[$i];

                if ($haystack === null) {
                    throw new InvalidArgumentException('StringMatch function requires non-null haystack');
                }

                if ($pattern === null) {
                    throw new InvalidArgumentException('StringMatch function requires non-null pattern');
                }

                try {
                    /** @var array<int|string, string> $result */
                    $result = s($haystack)->match($pattern);

                    $results[] = count($result) > 0 ? $result : null;
                } catch (Exception $e) {
                    throw new InvalidArgumentException('StringMatch error: ' . $e->getMessage(), 0, $e);
                }
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
