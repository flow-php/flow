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

/**
 * A PHP backend whose native memory is whatever the test reports - far above PHP's own allocations, so a memory limit
 * decides deterministically.
 */
final class ReportedMemoryBackend implements Backend
{
    public function __construct(
        public int $reported = 0,
        public readonly PhpBackend $php = new PhpBackend(),
        public readonly ?int $sortWorkingBytes = null,
        public int $reportedPeak = 0,
    ) {}

    public function adopt(Definition $definition, Column $column): Column
    {
        return $this->php->adopt($definition, $column);
    }

    public function allocatedBytes(): int
    {
        return $this->reported;
    }

    public function allocatedPeakBytes(): int
    {
        return $this->reportedPeak;
    }

    public function resetAllocatedPeak(): void
    {
        $this->reportedPeak = $this->reported;
    }

    public function builder(Definition $definition): ColumnBuilder
    {
        return $this->php->builder($definition);
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
        return $this->sortWorkingBytes ?? $this->php->sortWorkingBytes($rows, $columns);
    }

    public function groupedTotals(): GroupedTotals
    {
        return $this->php->groupedTotals();
    }

    public function groupedCounts(): GroupedCounts
    {
        return $this->php->groupedCounts();
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
        return $this->php->constant($definition, $value, $count);
    }

    public function decode(Definition $definition, array $buffers, int $count, int $nullCount): Column
    {
        return $this->php->decode($definition, $buffers, $count, $nullCount);
    }

    public function decodeFrame(string $body, Schema $schema, Codec $codec): Rows
    {
        return $this->php->decodeFrame($body, $schema, $codec);
    }

    public function encodeFrame(Rows $rows, Codec $codec): string
    {
        return $this->php->encodeFrame($rows, $codec);
    }
}
