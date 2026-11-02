<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Grouping\GroupTable;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Sort\SortColumn;
use Flow\Types\Type\Comparison\Operator;
use RuntimeException;
use SensitiveParameter;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class RustBackend implements Backend, Describable
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function adopt(Definition $definition, Column $column): Column
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function allocatedBytes(): int
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function builder(Definition $definition): ColumnBuilder
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function compare(
        Column $left,
        Operator $operator,
        Column $right,
        string $leftName,
        string $rightName,
    ): Column {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function isNull(Column $column): Column
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function isNotNull(Column $column): Column
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function add(Column $left, Column $right): Column
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function subtract(Column $left, Column $right): Column
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function multiply(Column $left, Column $right): Column
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function modulo(Column $left, Column $right): Column
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @param list<SortColumn> $columns
     */
    public function sort(Rows $rows, array $columns): Rows
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function groupedTotals(): GroupedTotals
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function groupedCounts(): GroupedCounts
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @param list<string> $columns
     */
    public function groupTable(array $columns, Schema $declared): GroupTable
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function constant(Definition $definition, mixed $value, int $count): Column
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function decode(Definition $definition, array $buffers, int $count, int $nullCount): Column
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function decodeFrame(string $body, Schema $schema, Codec $codec): Rows
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function describe(): ?Description
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function encodeFrame(Rows $rows, Codec $codec): string
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
