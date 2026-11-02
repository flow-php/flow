<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column;

use Flow\ETL\Foundation\Exception\ColumnMismatchException;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\OffsetOverflow;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Column\Frame\FloeException;
use Flow\ETL\Values\Column\Frame\FrameTooLarge;
use Flow\ETL\Values\Grouping\GroupTable;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Sort\SortColumn;
use Flow\Types\Type\Comparison\Operator;

use function interface_exists;

if (interface_exists(Backend::class, false)) {
    return;
}

interface Backend
{
    /**
     * @param Definition<mixed> $definition nullability, the enum class, the datetime zone and the cast plan
     */
    public function builder(Definition $definition): ColumnBuilder;

    /**
     * $value is logical or raw and is cast once through the Definition exactly like ColumnBuilder::append().
     *
     * @param Definition<mixed> $definition
     */
    public function constant(Definition $definition, mixed $value, int $count): Column;

    /**
     * Child node lengths and null counts are derived from the buffers, so only the top node travels.
     *
     * @param Definition<mixed> $definition
     * @param list<string> $buffers the column's subtree buffers as Column::encode() produced them ('' = omitted validity)
     */
    public function decode(Definition $definition, array $buffers, int $count, int $nullCount): Column;

    /**
     * A Floe BATCH frame body as Rows under $schema. $codec decodes the buffers stored with a length prefix (a prefix of
     * -1 is a raw buffer). The refusals are the frame's: truncated directory, counts other than the schema's, a column of
     * another row count, a buffer outside the area, a column whose buffers the backend refuses.
     *
     * @throws FloeException
     */
    public function decodeFrame(string $body, Schema $schema, Codec $codec): Rows;

    /**
     * $rows as one Floe BATCH frame body: the directory, then every column's buffers in schema order, each stored raw
     * (prefix -1) or through $codec with its raw length as prefix when that is shorter, 8-byte padded. The exact bytes
     * decodeFrame() reads back.
     *
     * @throws OffsetOverflow a column whose offsets do not fit the frame's u32 range
     * @throws FrameTooLarge a body past the 4 GiB frame length
     */
    public function encodeFrame(Rows $rows, Codec $codec): string;

    /**
     * @throws EvaluationException a value that does not cast to the type the pair compares in, named by $leftName or
     *                             $rightName
     * @throws InvalidArgumentException for Operator::DIFFERENT and Operator::SPACE_SHIP
     */
    public function compare(
        Column $left,
        Operator $operator,
        Column $right,
        string $leftName,
        string $rightName,
    ): Column;

    public function isNull(Column $column): Column;

    public function isNotNull(Column $column): Column;

    /**
     * @throws EvaluationException a null operand or an integer overflow
     */
    public function add(Column $left, Column $right): Column;

    /**
     * @throws EvaluationException a null operand or an integer overflow
     */
    public function subtract(Column $left, Column $right): Column;

    /**
     * @throws EvaluationException a null operand or an integer overflow
     */
    public function multiply(Column $left, Column $right): Column;

    /**
     * @throws EvaluationException a null operand, or a zero or negative modulus
     */
    public function modulo(Column $left, Column $right): Column;

    /**
     * @param list<SortColumn> $columns
     */
    public function sort(Rows $rows, array $columns): Rows;

    public function groupedTotals(): GroupedTotals;

    public function groupedCounts(): GroupedCounts;

    /**
     * @param list<string> $columns
     */
    public function groupTable(array $columns, Schema $declared): GroupTable;

    /**
     * The same values in this backend's storage: $column itself when this backend owns it, otherwise one copy through
     * builder($definition)->appendTake(). For producers that build columns outside the configured backend.
     *
     * @param Definition<mixed> $definition
     *
     * @throws ColumnMismatchException a null under NOT NULL on the copy path (an own column passes through unchecked;
     *                                 Rows::fromColumns checks NOT NULL)
     */
    public function adopt(Definition $definition, Column $column): Column;

    /**
     * Bytes this backend holds outside PHP's memory manager (invisible to memory_get_usage() and memory_limit).
     */
    public function allocatedBytes(): int;
}
