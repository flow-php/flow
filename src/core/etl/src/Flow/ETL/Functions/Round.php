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
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
        $this->precision = $precision instanceof ScalarFunction ? $precision : new Literal($precision);
        $this->mode = $mode instanceof ScalarFunction ? $mode : new Literal($mode);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            $rebuild->of($data['precision'] ?? null, ScalarFunction::class),
            $rebuild->of($data['mode'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
            'precision' => $this->precision,
            'mode' => $this->mode,
        ]);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $values = (new Parameter($this->value))->asNumbers($rows, $backend);
        $precisions = (new Parameter($this->precision))->asInts($rows, $backend);
        $modes = (new Parameter($this->mode))->asInts($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $precision = $precisions[$i];
                $mode = $modes[$i];

                if ($value === null || $precision === null || $mode === null) {
                    throw new InvalidArgumentException('Round function requires non-null values');
                }

                if ($mode < 1 || $mode > 4) {
                    $mode = 1;
                }

                $results[] = round($value, $precision, $mode);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
