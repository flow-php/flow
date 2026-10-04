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
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_string;
use function in_array;
use function is_string;
use function str_contains;

final class Contains implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $haystack;
    private readonly ScalarFunction $needle;

    public function __construct(ScalarFunction|string $haystack, ScalarFunction|string $needle)
    {
        $this->haystack = $haystack instanceof ScalarFunction ? $haystack : lit($haystack);
        $this->needle = $needle instanceof ScalarFunction ? $needle : lit($needle);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->haystack, $this->needle];
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
        return (new Nullability())->any(type_boolean(), $this->haystack->returns(), $this->needle->returns());
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $haystacks = (new Parameter($this->haystack))->asTypes($rows, $context, type_string(), type_array());
        $needles = (new Parameter($this->needle))->asStrings($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($haystacks as $i => $haystack) {
                $needle = $needles[$i];

                if ($haystack === null || $needle === null) {
                    $results[] = null;

                    continue;
                }

                if (is_string($haystack)) {
                    $results[] = str_contains($haystack, $needle);

                    continue;
                }

                $results[] = in_array($needle, $haystack, true);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
