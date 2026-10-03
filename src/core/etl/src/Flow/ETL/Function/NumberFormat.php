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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = (new Parameter($this->value))->asNumbers($rows, $context);
        $decimalsList = (new Parameter($this->decimals))->asInts($rows, $context);
        $decimalSeparators = (new Parameter($this->decimalSeparator))->asStrings($rows, $context);
        $thousandsSeparators = (new Parameter($this->thousandsSeparator))->asStrings($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $decimals = $decimalsList[$i];
                $decimalSeparator = $decimalSeparators[$i];
                $thousandsSeparator = $thousandsSeparators[$i];

                if (
                    $value === null
                    || $decimals === null
                    || $decimalSeparator === null
                    || $thousandsSeparator === null
                ) {
                    throw new InvalidArgumentException('NumberFormat function requires non-null values');
                }

                $results[] = number_format((float) $value, $decimals, $decimalSeparator, $thousandsSeparator);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
