<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column\Php;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\GroupedTotals;
use Flow\ETL\Values\Column\Physical\PhysicalFor;

use function abs;
use function count;
use function Flow\Types\DSL\type_float;
use function is_finite;
use function is_float;
use function is_int;
use function is_numeric;
use function is_string;
use function sprintf;

final class PhpGroupedTotals implements GroupedTotals
{
    /**
     * @var list<float>
     */
    private array $compensations = [];

    /**
     * @var list<int>
     */
    private array $counts = [];

    /**
     * @var list<float|int>
     */
    private array $sums = [];

    public function add(Column $values, Column $ids): void
    {
        $this->accumulate($ids->physicals(), $values->values());
    }

    public function sums(): Column
    {
        $sums = [];
        $nulls = 0;

        foreach ($this->sums as $id => $sum) {
            if ($this->counts[$id] === 0) {
                $sums[] = null;
                $nulls++;

                continue;
            }

            $sums[] = is_float($sum) && is_finite($sum) ? $sum + $this->compensations[$id] : (float) $sum;
        }

        return new ScalarColumn(type_float(), (new PhysicalFor())->physical(type_float()), $sums, $nulls);
    }

    public function absorbEarlier(GroupedTotals $earlier, array $into): void
    {
        $partials = $earlier->partials();

        foreach ($into as $earlierId => $id) {
            $partial = $partials[$earlierId] ?? throw new InvalidArgumentException(sprintf(
                'Earlier group %d has no totals',
                $earlierId,
            ));
            $this->grow($id);
            [$sum, $compensation, $count] = [$this->sums[$id], $this->compensations[$id], $this->counts[$id]];
            $this->sums[$id] = $partial['sum']['int'] ?? $partial['sum']['float'] ?? 0;
            $this->compensations[$id] = $partial['sum']['compensation'];
            $this->counts[$id] = $partial['count'];

            // NumericTotal::merge as earlier.merge(this): this group's sum is added onto the earlier one
            if ($count > 0) {
                $this->accumulate([$id], [$sum]);
                $this->compensations[$id] += $compensation;
                $this->counts[$id] = $partial['count'] + $count;
            }
        }
    }

    public function partials(): array
    {
        $partials = [];

        foreach ($this->sums as $id => $sum) {
            $partials[] = [
                'count' => $this->counts[$id],
                'sum' => [
                    'int' => is_int($sum) ? $sum : null,
                    'float' => is_float($sum) ? $sum : null,
                    'compensation' => $this->compensations[$id],
                ],
            ];
        }

        return $partials;
    }

    /**
     * @param array<int, mixed> $groups row => group id
     * @param array<int, mixed> $values row => value
     */
    private function accumulate(array $groups, array $values): void
    {
        [$sums, $compensations, $counts] = [$this->sums, $this->compensations, $this->counts];
        // moved out, not shared: a write to a shared array copies all of it, every group of every batch
        [$this->sums, $this->compensations, $this->counts] = [[], [], []];

        // RunningSum::addAll's non-exact step per group: an int overflow promotes to float, as PHP's + does
        // @mago-ignore analysis:mixed-assignment
        foreach ($values as $row => $value) {
            /** @var int $id */
            $id = $groups[$row];

            while (count($counts) <= $id) {
                $counts[] = 0;
                $sums[] = 0;
                $compensations[] = 0.0;
            }

            if (is_string($value) && is_numeric($value)) {
                $value = (float) $value;
            }

            if (!is_int($value) && !is_float($value)) {
                continue;
            }

            $counts[$id]++;
            $sum = $sums[$id];

            if (is_int($sum) && is_int($value)) {
                $sums[$id] = $sum + $value;

                continue;
            }

            $float = (float) $sum;
            $total = $float + $value;

            if (is_finite($total)) {
                $compensations[$id] += abs($float) >= abs($value) ? $float - $total + $value : $value - $total + $float;
            }

            $sums[$id] = $total;
        }

        $this->sums = $sums;
        $this->compensations = $compensations;
        $this->counts = $counts;
    }

    private function grow(int $id): void
    {
        while (count($this->counts) <= $id) {
            $this->counts[] = 0;
            $this->sums[] = 0;
            $this->compensations[] = 0.0;
        }
    }
}
