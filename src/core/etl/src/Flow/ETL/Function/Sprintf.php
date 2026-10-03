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
use function array_slice;
use function array_values;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_string;
use function in_array;
use function sprintf;

final class Sprintf implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var list<ScalarFunction>
     */
    private readonly array $values;

    private readonly ScalarFunction $format;

    public function __construct(ScalarFunction|string $format, ScalarFunction|float|int|string|null ...$values)
    {
        $this->format = $format instanceof ScalarFunction ? $format : lit($format);
        $this->values = array_values(array_map(
            static fn(ScalarFunction|float|int|string|null $value): ScalarFunction => $value instanceof ScalarFunction
                ? $value
                : lit($value),
            $values,
        ));
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->format, ...$this->values];
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
        $formats = (new Parameter($this->format))->asStrings($rows, $context);
        $arguments = array_map(static fn(ScalarFunction $value): array => (new Parameter($value))->values(
            $rows,
            $context,
        ), $this->values);
        $results = [];
        $i = 0;

        try {
            foreach ($formats as $i => $format) {
                /**
                 * @var array<null|float|int|string> $values
                 */
                $values = array_map(static fn(array $argument): mixed => $argument[$i], $arguments);

                if ($format === null || in_array(null, $values, true)) {
                    throw new InvalidArgumentException('Sprintf requires non-null format and values');
                }

                /** @var array<float|int|string> $nonNullValues */
                $nonNullValues = $values;

                $results[] = sprintf($format, ...$nonNullValues);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
