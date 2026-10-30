<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

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
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
        $this->decimals = $decimals instanceof ScalarFunction ? $decimals : new Literal($decimals);
        $this->decimalSeparator = $decimalSeparator instanceof ScalarFunction
            ? $decimalSeparator
            : new Literal($decimalSeparator);
        $this->thousandsSeparator = $thousandsSeparator instanceof ScalarFunction
            ? $thousandsSeparator
            : new Literal($thousandsSeparator);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            $rebuild->of($data['decimals'] ?? null, ScalarFunction::class),
            $rebuild->of($data['decimal_separator'] ?? null, ScalarFunction::class),
            $rebuild->of($data['thousands_separator'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
            'decimals' => $this->decimals,
            'decimal_separator' => $this->decimalSeparator,
            'thousands_separator' => $this->thousandsSeparator,
        ]);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $values = (new Parameter($this->value))->asNumbers($rows, $backend);
        $decimalsList = (new Parameter($this->decimals))->asInts($rows, $backend);
        $decimalSeparators = (new Parameter($this->decimalSeparator))->asStrings($rows, $backend);
        $thousandsSeparators = (new Parameter($this->thousandsSeparator))->asStrings($rows, $backend);
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

        return (new ResultColumn($backend))->of($this, $results);
    }
}
