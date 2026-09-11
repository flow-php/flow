<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_float;
use function round;

final class Round implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $precision;
    private readonly ScalarFunction $mode;

    public function __construct(
        ScalarFunction|int|float $value,
        ScalarFunction|int $precision = 2,
        ScalarFunction|int $mode = PHP_ROUND_HALF_UP,
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->precision = $precision instanceof ScalarFunction ? $precision : lit($precision);
        $this->mode = $mode instanceof ScalarFunction ? $mode : lit($mode);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->precision, $this->mode];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_float();
    }

    public function eval(Row $row, FlowContext $context): float
    {
        $value = (new Parameter($this->value))->asNumber($row, $context);
        $precision = (new Parameter($this->precision))->asInt($row, $context);
        $mode = (new Parameter($this->mode))->asInt($row, $context);

        if ($value === null || $precision === null || $mode === null) {
            throw new InvalidArgumentException('Round function requires non-null values');
        }

        if ($mode < 1 || $mode > 4) {
            $mode = 1;
        }

        return round($value, $precision, $mode);
    }
}
