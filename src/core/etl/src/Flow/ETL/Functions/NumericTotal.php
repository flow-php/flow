<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Calculator\Calculator;
use Flow\ETL\Foundation\Calculator\RunningSum;
use Flow\ETL\Foundation\Progress;
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
     * Per frame, the total aggregate() builds from that frame's rows.
     *
     * @param list<mixed> $values
     * @param list<array{0: int, 1: int}> $bounds inclusive indexes into $values; [1, 0] is an empty frame
     *
     * @return array{sums: list<null|float|int>, counts: list<int>} one sum and one count per bound
     */
    public static function frames(array $values, array $bounds, bool $exact, Progress $progress): array
    {
        $calculator = new Calculator();
        $runningSum = new RunningSum($calculator);
        $sums = [];
        $counts = [];
        $previous = null;
        $sum = null;
        $count = 0;
        $unreported = 0;

        foreach ($bounds as $bound) {
            if ($bound !== $previous) {
                [$start, $end] = $bound;

                if ($previous !== null && $start === $previous[0] && $end > $previous[1]) {
                    $count += $runningSum->addNumeric($values, $previous[1] + 1, $end, $exact);
                } else {
                    $runningSum = new RunningSum($calculator);
                    $count = $runningSum->addNumeric($values, $start, $end, $exact);
                }

                $sum = $count === 0 ? null : $runningSum->value();
                $previous = $bound;
            }

            $sums[] = $sum;
            $counts[] = $count;

            if (++$unreported === Progress::ROWS_PER_CALL) {
                $progress->advanced($unreported);
                $unreported = 0;
            }
        }

        $progress->advanced($unreported);

        return ['sums' => $sums, 'counts' => $counts];
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
