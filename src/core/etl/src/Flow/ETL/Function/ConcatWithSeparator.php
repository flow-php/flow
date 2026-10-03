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

use function array_map;
use function array_merge;
use function array_slice;
use function array_values;
use function Flow\ETL\DSL\is_type;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function implode;
use function is_string;

final class ConcatWithSeparator implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var list<ScalarFunction>
     */
    private readonly array $refs;

    private readonly ScalarFunction $separator;

    public function __construct(ScalarFunction|string $separator, ScalarFunction|string ...$refs)
    {
        $this->separator = $separator instanceof ScalarFunction ? $separator : lit($separator);
        $this->refs = array_values(array_map(static fn(ScalarFunction|string $ref): ScalarFunction => $ref
            instanceof ScalarFunction
                ? $ref
                : lit($ref), $refs));
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->separator, ...$this->refs];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], ...array_slice($children, 1));
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_string();
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $separators = (new Parameter($this->separator))->asStrings($rows, $context);
        $arguments = array_map(static fn(ScalarFunction $ref): array => (new Parameter($ref))->values(
            $rows,
            $context,
        ), $this->refs);
        $results = [];
        $i = 0;

        try {
            foreach ($separators as $i => $separator) {
                if (!is_string($separator)) {
                    throw new InvalidArgumentException('ConcatWithSeparator function requires non-null separator');
                }

                /** @var array<string> $concatValues */
                $concatValues = [];

                foreach ($arguments as $argument) {
                    // @mago-ignore analysis:mixed-assignment
                    $value = $argument[$i];

                    if (is_type(type_list(type_string()), $value)) {
                        /** @var list<string> $value */
                        $concatValues = array_merge($concatValues, $value);
                    } elseif ($value !== null) {
                        $concatValues[] = type_string()->cast($value);
                    }
                }

                $results[] = implode($separator, $concatValues);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
