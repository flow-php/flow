<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column\Php;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\GroupedCounts;
use Flow\ETL\Values\Column\Physical\PhysicalFor;

use function array_count_values;
use function count;
use function Flow\Types\DSL\type_integer;
use function max;
use function sprintf;

final class PhpGroupedCounts implements GroupedCounts
{
    /**
     * @var list<int>
     */
    private array $counts = [];

    public function add(Column $values, Column $ids): void
    {
        /** @var list<int> $groups */
        $groups = $ids->physicals();

        if ($groups === []) {
            return;
        }

        $this->grow(max($groups));

        if ($values->nullCount() > 0) {
            foreach ($groups as $row => $_) {
                if ($values->isNull($row)) {
                    unset($groups[$row]);
                }
            }
        }

        foreach (array_count_values($groups) as $id => $count) {
            $this->counts[$id] += $count;
        }
    }

    public function counts(): Column
    {
        return new ScalarColumn(type_integer(), (new PhysicalFor())->physical(type_integer()), $this->counts, 0);
    }

    public function partials(): array
    {
        return $this->counts;
    }

    public function absorbEarlier(GroupedCounts $earlier, array $into): void
    {
        $counts = $earlier->partials();

        foreach ($into as $from => $to) {
            $this->grow($to);
            $this->counts[$to] += $counts[$from] ?? throw new InvalidArgumentException(sprintf(
                'Earlier group %d has no count',
                $from,
            ));
        }
    }

    private function grow(int $id): void
    {
        while (count($this->counts) <= $id) {
            $this->counts[] = 0;
        }
    }
}
