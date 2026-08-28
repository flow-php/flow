<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
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

    public function eval(Row $row, FlowContext $context): ?string
    {
        $format = (new Parameter($this->format))->asString($row, $context);

        /**
         * @var array<null|float|int|string> $values
         */
        $values = array_map(static fn(ScalarFunction $value): mixed => (new Parameter($value))->eval(
            $row,
            $context,
        ), $this->values);

        if ($format === null || in_array(null, $values, true)) {
            throw new InvalidArgumentException('Sprintf requires non-null format and values');
        }

        /** @var array<float|int|string> $nonNullValues */
        $nonNullValues = $values;

        return sprintf($format, ...$nonNullValues);
    }
}
