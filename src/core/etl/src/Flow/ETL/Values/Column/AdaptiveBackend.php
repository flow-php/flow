<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\FlowPhpExtension;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Grouping\GroupTable;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Sort\SortColumn;
use Flow\Types\Type\Comparison\Operator;
use SensitiveParameter;

final readonly class AdaptiveBackend implements Backend, Describable
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self();
    }

    public function describe(): ?Description
    {
        return new Description(self::class, []);
    }

    private Backend $backend;

    public function __construct(?FlowPhpExtension $extension = null)
    {
        $this->backend = ($extension ?? FlowPhpExtension::detect())->available() ? new RustBackend() : new PhpBackend();
    }

    public function builder(Definition $definition): ColumnBuilder
    {
        return $this->backend->builder($definition);
    }

    public function constant(Definition $definition, mixed $value, int $count): Column
    {
        return $this->backend->constant($definition, $value, $count);
    }

    public function decode(Definition $definition, array $buffers, int $count, int $nullCount): Column
    {
        return $this->backend->decode($definition, $buffers, $count, $nullCount);
    }

    public function decodeFrame(string $body, Schema $schema, Codec $codec): Rows
    {
        return $this->backend->decodeFrame($body, $schema, $codec);
    }

    public function encodeFrame(Rows $rows, Codec $codec): string
    {
        return $this->backend->encodeFrame($rows, $codec);
    }

    public function compare(
        Column $left,
        Operator $operator,
        Column $right,
        string $leftName,
        string $rightName,
    ): Column {
        return $this->backend->compare($left, $operator, $right, $leftName, $rightName);
    }

    public function isNull(Column $column): Column
    {
        return $this->backend->isNull($column);
    }

    public function isNotNull(Column $column): Column
    {
        return $this->backend->isNotNull($column);
    }

    public function add(Column $left, Column $right): Column
    {
        return $this->backend->add($left, $right);
    }

    public function subtract(Column $left, Column $right): Column
    {
        return $this->backend->subtract($left, $right);
    }

    public function multiply(Column $left, Column $right): Column
    {
        return $this->backend->multiply($left, $right);
    }

    public function modulo(Column $left, Column $right): Column
    {
        return $this->backend->modulo($left, $right);
    }

    /**
     * @param list<SortColumn> $columns
     */
    public function sort(Rows $rows, array $columns): Rows
    {
        return $this->backend->sort($rows, $columns);
    }

    public function sortWorkingBytes(Rows $rows, array $columns): int
    {
        return $this->backend->sortWorkingBytes($rows, $columns);
    }

    public function groupedTotals(): GroupedTotals
    {
        return $this->backend->groupedTotals();
    }

    public function groupedCounts(): GroupedCounts
    {
        return $this->backend->groupedCounts();
    }

    /**
     * @param list<string> $columns
     */
    public function groupTable(array $columns, Schema $declared): GroupTable
    {
        return $this->backend->groupTable($columns, $declared);
    }

    public function adopt(Definition $definition, Column $column): Column
    {
        return $this->backend->adopt($definition, $column);
    }

    public function allocatedBytes(): int
    {
        return $this->backend->allocatedBytes();
    }

    public function allocatedPeakBytes(): int
    {
        return $this->backend->allocatedPeakBytes();
    }

    public function resetAllocatedPeak(): void
    {
        $this->backend->resetAllocatedPeak();
    }
}
