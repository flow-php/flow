<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\ColumnBuilder;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Column\GroupedCounts;
use Flow\ETL\Values\Column\GroupedTotals;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Grouping\GroupTable;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Sort\SortColumn;
use Flow\Types\Type\Comparison\Operator;

final class SpyBackend implements Backend
{
    private int $adopts = 0;

    private int $builders = 0;

    private int $constants = 0;

    private int $decodeFrames = 0;

    private int $groupedCounts = 0;

    private int $groupedTotals = 0;

    private readonly PhpBackend $php;

    public function __construct(
        private readonly int $allocatedBytes = 0,
    ) {
        $this->php = new PhpBackend();
    }

    public function adopt(Definition $definition, Column $column): Column
    {
        $this->adopts++;

        return $this->php->adopt($definition, $column);
    }

    public function adopts(): int
    {
        return $this->adopts;
    }

    public function allocatedBytes(): int
    {
        return $this->allocatedBytes;
    }

    public function allocatedPeakBytes(): int
    {
        return 0;
    }

    public function resetAllocatedPeak(): void {}

    public function builder(Definition $definition): ColumnBuilder
    {
        $this->builders++;

        return $this->php->builder($definition);
    }

    public function builders(): int
    {
        return $this->builders;
    }

    public function decodeFrame(string $body, Schema $schema, Codec $codec): Rows
    {
        $this->decodeFrames++;

        return $this->php->decodeFrame($body, $schema, $codec);
    }

    public function decodeFrames(): int
    {
        return $this->decodeFrames;
    }

    public function encodeFrame(Rows $rows, Codec $codec): string
    {
        return $this->php->encodeFrame($rows, $codec);
    }

    public function compare(
        Column $left,
        Operator $operator,
        Column $right,
        string $leftName,
        string $rightName,
    ): Column {
        return $this->php->compare($left, $operator, $right, $leftName, $rightName);
    }

    public function isNull(Column $column): Column
    {
        return $this->php->isNull($column);
    }

    public function isNotNull(Column $column): Column
    {
        return $this->php->isNotNull($column);
    }

    public function add(Column $left, Column $right): Column
    {
        return $this->php->add($left, $right);
    }

    public function subtract(Column $left, Column $right): Column
    {
        return $this->php->subtract($left, $right);
    }

    public function multiply(Column $left, Column $right): Column
    {
        return $this->php->multiply($left, $right);
    }

    public function modulo(Column $left, Column $right): Column
    {
        return $this->php->modulo($left, $right);
    }

    /**
     * @param list<SortColumn> $columns
     */
    public function sort(Rows $rows, array $columns): Rows
    {
        return $this->php->sort($rows, $columns);
    }

    /**
     * @param list<SortColumn> $columns
     */
    public function sortWorkingBytes(Rows $rows, array $columns): int
    {
        return $this->php->sortWorkingBytes($rows, $columns);
    }

    public function groupedTotals(): GroupedTotals
    {
        $this->groupedTotals++;

        return $this->php->groupedTotals();
    }

    public function groupedTotalsCreated(): int
    {
        return $this->groupedTotals;
    }

    public function groupedCounts(): GroupedCounts
    {
        $this->groupedCounts++;

        return $this->php->groupedCounts();
    }

    public function groupedCountsCreated(): int
    {
        return $this->groupedCounts;
    }

    /**
     * @param list<string> $columns
     */
    public function groupTable(array $columns, Schema $declared): GroupTable
    {
        return $this->php->groupTable($columns, $declared);
    }

    public function constant(Definition $definition, mixed $value, int $count): Column
    {
        $this->constants++;

        return $this->php->constant($definition, $value, $count);
    }

    public function constants(): int
    {
        return $this->constants;
    }

    public function decode(Definition $definition, array $buffers, int $count, int $nullCount): Column
    {
        return $this->php->decode($definition, $buffers, $count, $nullCount);
    }
}
