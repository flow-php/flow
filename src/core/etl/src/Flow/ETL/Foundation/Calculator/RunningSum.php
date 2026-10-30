<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Calculator;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\Types\Type\Logical\StructureType;

use function abs;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_structure;
use function is_finite;
use function is_float;
use function is_int;
use function is_string;

/**
 * IEEE addition with Neumaier compensation: the rounding error of every float addition is carried and added back once,
 * so a long run of floats does not drift. An exact addition is decimal (Calculator) over the compensated total.
 */
final class RunningSum
{
    private float $compensation = 0.0;

    private float|int $sum = 0;

    public function __construct(
        private readonly Calculator $calculator = new Calculator(),
    ) {}

    /**
     * @param array<array-key, mixed> $partial the shape of partial()
     *
     * @throws InvalidArgumentException when both or neither of int and float are set
     */
    public static function fromPartial(array $partial, Calculator $calculator = new Calculator()): self
    {
        $data = self::partialType()->assert($partial);
        $int = type_optional(type_integer())->assert($data['int']);
        $float = type_optional(type_float())->assert($data['float']);

        if (($int === null) === ($float === null)) {
            throw new InvalidArgumentException('A running sum partial holds exactly one of int and float');
        }

        $sum = new self($calculator);
        $sum->sum = $int ?? $float ?? 0;
        $sum->compensation = type_float()->assert($data['compensation']);

        return $sum;
    }

    /**
     * @return array{int: int, float: null, compensation: float} the partial of a sum nothing was added to
     */
    public static function emptyPartial(): array
    {
        return ['int' => 0, 'float' => null, 'compensation' => 0.0];
    }

    public static function partialType(): StructureType
    {
        return type_structure([
            'int' => type_optional(type_integer()),
            'float' => type_optional(type_float()),
            'compensation' => type_float(),
        ]);
    }

    /**
     * @return array{int: null|int, float: null|float, compensation: float} exactly one of int and float is set
     */
    public function partial(): array
    {
        return [
            'int' => is_int($this->sum) ? $this->sum : null,
            'float' => is_float($this->sum) ? $this->sum : null,
            'compensation' => $this->compensation,
        ];
    }

    /**
     * @param float|int|numeric-string $value
     */
    public function add(float|int|string $value, bool $exact): void
    {
        if ($exact) {
            $this->sum = $this->calculator->add($this->value(), $value);
            $this->compensation = 0.0;

            return;
        }

        $value = is_string($value) ? (float) $value : $value;

        if (is_int($this->sum) && is_int($value)) {
            // an int overflow promotes to float, as PHP's + does
            $this->sum += $value;

            return;
        }

        $sum = (float) $this->sum;
        $total = $sum + $value;

        if (is_finite($total)) {
            $this->compensation += abs($sum) >= abs($value) ? $sum - $total + $value : $value - $total + $sum;
        }

        $this->sum = $total;
    }

    /**
     * add() over every value, the compensated loop inlined: one call per batch, not per value.
     *
     * @param array<array-key, float|int|numeric-string> $values
     */
    public function addAll(array $values, bool $exact): void
    {
        if ($exact) {
            foreach ($values as $value) {
                $this->add($value, true);
            }

            return;
        }

        $sum = $this->sum;
        $compensation = $this->compensation;

        foreach ($values as $value) {
            if (is_string($value)) {
                $value = (float) $value;
            }

            if (is_int($sum) && is_int($value)) {
                // an int overflow promotes to float, as PHP's + does
                $sum += $value;

                continue;
            }

            $float = (float) $sum;
            $total = $float + $value;

            if (is_finite($total)) {
                $compensation += abs($float) >= abs($value) ? $float - $total + $value : $value - $total + $float;
            }

            $sum = $total;
        }

        $this->sum = $sum;
        $this->compensation = $compensation;
    }

    public function merge(self $other, bool $exact): void
    {
        if ($exact) {
            $this->add($other->value(), true);

            return;
        }

        $this->add($other->sum, false);
        $this->compensation += $other->compensation;
    }

    public function value(): float|int
    {
        return is_float($this->sum) && is_finite($this->sum) ? $this->sum + $this->compensation : $this->sum;
    }
}
