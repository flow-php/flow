<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_string;
use function number_format;

final class NumberFormat implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $decimals;
    private readonly ScalarFunction $decimalSeparator;
    private readonly ScalarFunction $thousandsSeparator;

    public function __construct(
        ScalarFunction|int|float $value,
        ScalarFunction|int $decimals,
        ScalarFunction|string $decimalSeparator = '.',
        ScalarFunction|string $thousandsSeparator = ',',
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->decimals = $decimals instanceof ScalarFunction ? $decimals : lit($decimals);
        $this->decimalSeparator = $decimalSeparator instanceof ScalarFunction
            ? $decimalSeparator
            : lit($decimalSeparator);
        $this->thousandsSeparator = $thousandsSeparator instanceof ScalarFunction
            ? $thousandsSeparator
            : lit($thousandsSeparator);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->decimals, $this->decimalSeparator, $this->thousandsSeparator];
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
        return type_string();
    }

    public function eval(Row $row, FlowContext $context): ?string
    {
        $value = (new Parameter($this->value))->asNumber($row, $context);
        $decimals = (new Parameter($this->decimals))->asInt($row, $context);
        $decimalSeparator = (new Parameter($this->decimalSeparator))->asString($row, $context);
        $thousandsSeparator = (new Parameter($this->thousandsSeparator))->asString($row, $context);

        if ($value === null || $decimals === null || $decimalSeparator === null || $thousandsSeparator === null) {
            throw new InvalidArgumentException('NumberFormat function requires non-null values');
        }

        return number_format((float) $value, $decimals, $decimalSeparator, $thousandsSeparator);
    }
}
