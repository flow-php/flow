<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Calculator\Calculator;
use Flow\ETL\Foundation\Calculator\RunningSum;
use Flow\ETL\Values\Column\Column;
use Flow\Types\Type\Logical\StructureType;

use function count;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_structure;
use function is_float;
use function is_int;
use function is_numeric;
use function is_string;

final class NumericTotal
{
    private int $count = 0;

    private ?RunningSum $runningSum = null;

    public function __construct(
        public readonly bool $exact = false,
    ) {}

    /**
     * @return StructureType<array<string, mixed>>
     */
    public static function partialType(): StructureType
    {
        return type_structure([
            'count' => type_integer(),
            'sum' => RunningSum::partialType(),
        ]);
    }

    /**
     * @param list<int> $indices
     */
    public function aggregate(Column $column, array $indices): void
    {
        if (count($indices) === 1) {
            // @mago-ignore analysis:mixed-assignment
            $value = $column->value($indices[0]);

            if (is_int($value) || is_float($value) || is_string($value) && is_numeric($value)) {
                $this->runningSum ??= new RunningSum(new Calculator());
                $this->runningSum->add($value, $this->exact);
                $this->count++;
            }

            return;
        }

        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($column->take($indices)->values() as $value) {
            if (is_int($value) || is_float($value) || is_string($value) && is_numeric($value)) {
                $values[] = $value;
            }
        }

        if ($values === []) {
            return;
        }

        $this->runningSum ??= new RunningSum(new Calculator());
        $this->runningSum->addAll($values, $this->exact);
        $this->count += count($values);
    }

    public function count(): int
    {
        return $this->count;
    }

    /**
     * @param array<array-key, mixed> $partial
     */
    public function merge(array $partial): void
    {
        $data = self::partialType()->assert($partial);
        $count = type_integer()->assert($data['count']);

        if ($count === 0) {
            return;
        }

        $this->runningSum ??= new RunningSum(new Calculator());
        $this->runningSum->merge(
            RunningSum::fromPartial(type_array()->assert($data['sum']), new Calculator()),
            $this->exact,
        );
        $this->count += $count;
    }

    /**
     * @return array{count: int, sum: array{int: null|int, float: null|float, compensation: float}}
     */
    public function partial(): array
    {
        return [
            'count' => $this->count,
            'sum' => $this->runningSum?->partial() ?? RunningSum::emptyPartial(),
        ];
    }

    public function sum(): float|int|null
    {
        return $this->count === 0 ? null : $this->runningSum?->value();
    }
}
