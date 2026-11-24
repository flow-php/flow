<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Hash\HashAlgorithm;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\ColumnBuilder;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Column\GroupedCounts;
use Flow\ETL\Values\Column\GroupedMultisetHashes;
use Flow\ETL\Values\Column\GroupedTotals;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Grouping\GroupTable;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Sort\SortColumn;
use Flow\Types\Type\Comparison\Operator;

/**
 * A backend whose builders take any value, so a replay through it finds no violation.
 */
final readonly class AcceptingBackend implements Backend
{
    public function builder(Definition $definition): ColumnBuilder
    {
        return new AcceptingColumnBuilder();
    }

    public function compare(
        Column $left,
        Operator $operator,
        Column $right,
        string $leftName,
        string $rightName,
    ): Column {
        return (new PhpBackend())->compare($left, $operator, $right, $leftName, $rightName);
    }

    public function isNull(Column $column): Column
    {
        return (new PhpBackend())->isNull($column);
    }

    public function isNotNull(Column $column): Column
    {
        return (new PhpBackend())->isNotNull($column);
    }

    public function add(Column $left, Column $right): Column
    {
        return (new PhpBackend())->add($left, $right);
    }

    public function subtract(Column $left, Column $right): Column
    {
        return (new PhpBackend())->subtract($left, $right);
    }

    public function multiply(Column $left, Column $right): Column
    {
        return (new PhpBackend())->multiply($left, $right);
    }

    public function modulo(Column $left, Column $right): Column
    {
        return (new PhpBackend())->modulo($left, $right);
    }

    public function startsWith(Column $haystack, Column $needle): Column
    {
        return (new PhpBackend())->startsWith($haystack, $needle);
    }

    public function endsWith(Column $haystack, Column $needle): Column
    {
        return (new PhpBackend())->endsWith($haystack, $needle);
    }

    public function contains(Column $haystack, Column $needle): Column
    {
        return (new PhpBackend())->contains($haystack, $needle);
    }

    /**
     * @param list<SortColumn> $columns
     */
    public function sort(Rows $rows, array $columns): Rows
    {
        return (new PhpBackend())->sort($rows, $columns);
    }

    /**
     * @param list<SortColumn> $columns
     */
    public function sortWorkingBytes(Rows $rows, array $columns): int
    {
        return (new PhpBackend())->sortWorkingBytes($rows, $columns);
    }

    public function groupedTotals(): GroupedTotals
    {
        return (new PhpBackend())->groupedTotals();
    }

    public function groupedCounts(): GroupedCounts
    {
        return (new PhpBackend())->groupedCounts();
    }

    public function rowHashes(array $columns, int $count): Column
    {
        return (new PhpBackend())->rowHashes($columns, $count);
    }

    public function hash(Column $strings, HashAlgorithm $algorithm): Column
    {
        return (new PhpBackend())->hash($strings, $algorithm);
    }

    public function groupedMultisetHashes(HashAlgorithm $algorithm): GroupedMultisetHashes
    {
        return (new PhpBackend())->groupedMultisetHashes($algorithm);
    }

    /**
     * @param list<string> $columns
     */
    public function groupTable(array $columns, Schema $declared): GroupTable
    {
        return (new PhpBackend())->groupTable($columns, $declared);
    }

    public function partition(Rows $rows, array $keys, int $buckets, int $hashOffset): array
    {
        return (new PhpBackend())->partition($rows, $keys, $buckets, $hashOffset);
    }

    public function constant(Definition $definition, mixed $value, int $count): Column
    {
        return (new PhpBackend())->constant($definition, $value, $count);
    }

    public function decode(Definition $definition, array $buffers, int $count, int $nullCount): Column
    {
        return (new PhpBackend())->decode($definition, $buffers, $count, $nullCount);
    }

    public function decodeFrame(string $body, Schema $schema, Codec $codec): Rows
    {
        return (new PhpBackend())->decodeFrame($body, $schema, $codec);
    }

    public function encodeFrame(Rows $rows, Codec $codec): string
    {
        return (new PhpBackend())->encodeFrame($rows, $codec);
    }

    public function adopt(Definition $definition, Column $column): Column
    {
        return $column;
    }

    public function allocatedBytes(): int
    {
        return 0;
    }

    public function allocatedPeakBytes(): int
    {
        return 0;
    }

    public function resetAllocatedPeak(): void {}

    public function releaseFreedMemory(): void {}
}
