<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column;

use Exception;
use Flow\ETL\Foundation\Calculator\Calculator;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\ColumnMismatchException;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Column\Frame\FloeException;
use Flow\ETL\Values\Column\Frame\FrameTooLarge;
use Flow\ETL\Values\Column\Layout\BufferLayout;
use Flow\ETL\Values\Column\Layout\Buffers;
use Flow\ETL\Values\Column\Php\CastingColumnBuilder;
use Flow\ETL\Values\Column\Php\ColumnDecoder;
use Flow\ETL\Values\Column\Php\ConstantColumn;
use Flow\ETL\Values\Column\Php\ListColumn;
use Flow\ETL\Values\Column\Php\MapColumn;
use Flow\ETL\Values\Column\Php\PhpGroupedCounts;
use Flow\ETL\Values\Column\Php\PhpGroupedTotals;
use Flow\ETL\Values\Column\Php\ScalarColumn;
use Flow\ETL\Values\Column\Php\StructColumn;
use Flow\ETL\Values\Column\Physical\PhysicalFor;
use Flow\ETL\Values\Grouping\GroupTable;
use Flow\ETL\Values\Grouping\PhpGroupTable;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definition\NullDefinition;
use Flow\ETL\Values\Schema\Definitions;
use Flow\ETL\Values\Sort\RowOrder;
use Flow\Types\Type\Comparison\Operator;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use SensitiveParameter;

use function array_keys;
use function array_map;
use function array_sum;
use function array_values;
use function count;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function intdiv;
use function is_float;
use function is_int;
use function pack;
use function range;
use function sprintf;
use function str_repeat;
use function strlen;
use function substr;
use function unpack;

use const SORT_NUMERIC;
use const SORT_STRING;

final readonly class PhpBackend implements Backend, Describable
{
    private BufferLayout $layout;

    public function __construct()
    {
        $this->layout = new BufferLayout();
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self();
    }

    public function describe(): ?Description
    {
        return new Description(self::class, []);
    }

    public function builder(Definition $definition): ColumnBuilder
    {
        return new CastingColumnBuilder(
            $definition,
            (new PhysicalFor())->definition($definition),
            (new PhysicalFor())->builder($definition->type()),
        );
    }

    public function constant(Definition $definition, mixed $value, int $count): Column
    {
        $physical = (new PhysicalFor())->definition($definition);

        if ($value === null) {
            if (!$definition->isNullable() && !$definition instanceof NullDefinition) {
                throw ColumnMismatchException::valueDoesNotMatch(
                    $definition->name(),
                    $definition->type()->toString(),
                    null,
                );
            }

            return new ConstantColumn($definition->type(), $physical, null, $count);
        }

        return new ConstantColumn(
            $definition->type(),
            $physical,
            $physical->toPhysical($definition->type()->cast($value)),
            $count,
        );
    }

    public function decode(Definition $definition, array $buffers, int $count, int $nullCount): Column
    {
        $cursor = new Buffers($buffers);
        $column = (new ColumnDecoder())->decode($definition->type(), $cursor, $count, $nullCount);

        if ($cursor->remaining() !== 0) {
            throw new InvalidArgumentException(sprintf(
                'Column "%s": %d buffers left after decoding',
                $definition->name(),
                $cursor->remaining(),
            ));
        }

        return $column;
    }

    public function decodeFrame(string $body, Schema $schema, Codec $codec): Rows
    {
        if (strlen($body) < 12) {
            throw new FloeException('Floe BATCH frame is truncated, its directory is incomplete');
        }

        /** @var array{1: int, 2: int, 3: int} $counts */
        $counts = unpack('V3', $body);
        [1 => $rowCount, 2 => $nodeCount, 3 => $bufferCount] = $counts;
        $nodeCounts = [];
        $bufferCounts = [];

        foreach ($schema->definitions() as $name => $definition) {
            $nodeCounts[$name] = $this->layout->nodeCount($definition->type());
            $bufferCounts[$name] = $this->layout->bufferCount($definition->type());
        }

        $expectedNodes = array_sum($nodeCounts);
        $expectedBuffers = array_sum($bufferCounts);

        if ($nodeCount !== $expectedNodes) {
            throw new FloeException(sprintf(
                'Floe BATCH frame holds %d nodes, the schema describes %d',
                $nodeCount,
                $expectedNodes,
            ));
        }

        if ($bufferCount !== $expectedBuffers) {
            throw new FloeException(sprintf(
                'Floe BATCH frame holds %d buffers, the schema describes %d',
                $bufferCount,
                $expectedBuffers,
            ));
        }

        $areaStart = intdiv(12 + (8 * ($nodeCount + $bufferCount)) + 7, 8) * 8;

        if (strlen($body) < $areaStart) {
            throw new FloeException('Floe BATCH frame is truncated, its directory is incomplete');
        }

        /** @var list<int> $words */
        $words = ($nodeCount + $bufferCount) === 0
            ? []
            : array_values((array) unpack('V' . (2 * ($nodeCount + $bufferCount)), $body, 12));
        $area = substr($body, $areaStart);
        $areaLength = strlen($area);
        $node = 0;
        $extent = 2 * $nodeCount;
        $buffer = 0;
        $columns = [];

        foreach ($schema->definitions() as $name => $definition) {
            $type = $definition->type();
            $length = $words[2 * $node];
            $nullCount = $words[(2 * $node) + 1];

            if ($length !== $rowCount) {
                throw new FloeException(sprintf(
                    'Floe BATCH frame column "%s" holds %d rows, the frame %d',
                    $name,
                    $length,
                    $rowCount,
                ));
            }

            if ($nullCount > $length) {
                throw new FloeException(sprintf(
                    'Floe BATCH frame column "%s" has %d nulls in %d rows',
                    $name,
                    $nullCount,
                    $length,
                ));
            }

            $buffers = [];

            for ($i = $bufferCounts[$name]; $i > 0; $i--, $buffer++, $extent += 2) {
                $offset = $words[$extent];
                $stored = $words[$extent + 1];

                if ($stored === 0) {
                    $buffers[] = '';

                    continue;
                }

                if (($offset + $stored) > $areaLength) {
                    throw new FloeException(sprintf(
                        'Floe BATCH frame buffer %d lies outside the %d-byte buffer area',
                        $buffer,
                        $areaLength,
                    ));
                }

                if ($stored < 8) {
                    throw new FloeException(sprintf(
                        'Floe BATCH frame buffer %d is shorter than its 8-byte length prefix',
                        $buffer,
                    ));
                }

                /** @var array{1: int} $prefix */
                $prefix = unpack('P', $area, $offset);
                $bytes = substr($area, $offset + 8, $stored - 8);

                if ($prefix[1] === -1) {
                    $buffers[] = $bytes;

                    continue;
                }

                $decoded = $codec->decode($bytes);

                if (strlen($decoded) !== $prefix[1]) {
                    throw new FloeException(sprintf(
                        'Floe BATCH frame buffer %d decodes to %d bytes, its prefix declares %d',
                        $buffer,
                        strlen($decoded),
                        $prefix[1],
                    ));
                }

                $buffers[] = $decoded;
            }

            try {
                // a flat column's only node is its top node, already checked above; the backend checks its buffers
                if ($nodeCounts[$name] === 1) {
                    $node++;
                    $columns[$name] = $this->decode($definition, $buffers, $rowCount, $nullCount);

                    continue;
                }

                foreach ($this->layout->nodes($type, $length, $nullCount, $buffers) as [
                    $derivedLength,
                    $derivedNulls,
                ]) {
                    if ($words[2 * $node] !== $derivedLength || $words[(2 * $node) + 1] !== $derivedNulls) {
                        throw new FloeException(sprintf(
                            'Floe BATCH frame column "%s" declares nodes that disagree with its buffers',
                            $name,
                        ));
                    }

                    $node++;
                }

                $columns[$name] = $this->decode($definition, $buffers, $rowCount, $nullCount);
            } catch (InvalidArgumentException $e) {
                throw new FloeException(
                    sprintf('Floe BATCH frame column "%s" is malformed: %s', $name, $e->getMessage()),
                    0,
                    $e,
                );
            }
        }

        try {
            return Rows::fromColumns($schema, $columns, $rowCount);
        } catch (SchemaMismatchException $e) {
            throw new FloeException(sprintf('Floe BATCH frame is malformed: %s', $e->getMessage()), 0, $e);
        }
    }

    public function encodeFrame(Rows $rows, Codec $codec): string
    {
        $nodes = '';
        $extents = '';
        $area = '';
        $nodeCount = 0;
        $bufferCount = 0;

        foreach ($rows->schema()->definitions() as $definition) {
            $column = $rows->column($definition->name());
            $buffers = $column->encode();

            $columnNodes = $this->layout->nodeCount($definition->type()) === 1
                ? [[$rows->count(), $column->nullCount()]]
                : $this->layout->nodes($definition->type(), $rows->count(), $column->nullCount(), $buffers);

            foreach ($columnNodes as [$length, $nulls]) {
                $nodes .= pack('VV', $length, $nulls);
                $nodeCount++;
            }

            foreach ($buffers as $buffer) {
                $bufferCount++;

                if ($buffer === '') {
                    $extents .= pack('VV', 0, 0);

                    continue;
                }

                $encoded = $codec->encode($buffer);
                $stored = strlen($encoded) < strlen($buffer)
                    ? pack('P', strlen($buffer)) . $encoded
                    : pack('P', -1) . $buffer;
                $extents .= pack('VV', strlen($area), strlen($stored));
                $area .= $stored . str_repeat("\0", (8 - (strlen($stored) % 8)) % 8);
            }
        }

        $directory = pack('VVV', $rows->count(), $nodeCount, $bufferCount) . $nodes . $extents;
        $body = $directory . str_repeat("\0", (8 - (strlen($directory) % 8)) % 8) . $area;

        if (strlen($body) > 0xFFFFFFFF) {
            throw new FrameTooLarge(sprintf(
                'Floe BATCH frame body of %d bytes exceeds the 4 GiB frame length',
                strlen($body),
            ));
        }

        return $body;
    }

    public function compare(
        Column $left,
        Operator $operator,
        Column $right,
        string $leftName,
        string $rightName,
    ): Column {
        $order = new ValueOrder($left->type(), $right->type());
        $comparable = new ComparableValues();
        $results = match ($operator) {
            Operator::EQUAL, Operator::NOT_EQUAL => (new ValueEquality())->equal($left, $right, $leftName, $rightName),
            Operator::IDENTICAL, Operator::NOT_IDENTICAL => (new ValueEquality())->identical($left, $right),
            Operator::GREATER_THAN => $order->lessThan(...$comparable->orderings($right, $left)),
            Operator::GREATER_THAN_EQUAL => $order->lessThanOrEqual(...$comparable->orderings($right, $left)),
            Operator::LESS_THAN => $order->lessThan(...$comparable->orderings($left, $right)),
            Operator::LESS_THAN_EQUAL => $order->lessThanOrEqual(...$comparable->orderings($left, $right)),
            Operator::DIFFERENT, Operator::SPACE_SHIP => throw new InvalidArgumentException(
                'Comparison was built with an unsupported operator',
            ),
        };

        if ($operator === Operator::NOT_EQUAL || $operator === Operator::NOT_IDENTICAL) {
            foreach ($results as $i => $result) {
                $results[$i] = $result === null ? null : !$result;
            }
        }

        $definition = Definitions::of('value', type_boolean(), true);

        return new ScalarColumn(
            $definition->type(),
            (new PhysicalFor())->definition($definition),
            $results,
            count(array_keys($results, null, true)),
        );
    }

    public function isNull(Column $column): Column
    {
        return $this->nullTest($column, true);
    }

    public function isNotNull(Column $column): Column
    {
        return $this->nullTest($column, false);
    }

    public function add(Column $left, Column $right): Column
    {
        $rights = $right->physicals();
        $results = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($left->physicals() as $i => $leftValue) {
                // @mago-ignore analysis:mixed-assignment
                $rightValue = $rights[$i];

                if (!(is_int($leftValue) || is_float($leftValue)) || !(is_int($rightValue) || is_float($rightValue))) {
                    throw new InvalidArgumentException('Plus function requires non-null values');
                }

                $result = $leftValue + $rightValue;

                if (is_int($leftValue) && is_int($rightValue) && is_float($result)) {
                    throw new InvalidArgumentException('Plus function integer overflow');
                }

                $results[] = $result;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return $this->arithmeticResult($left, $right, $results);
    }

    public function subtract(Column $left, Column $right): Column
    {
        $rights = $right->physicals();
        $results = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($left->physicals() as $i => $leftValue) {
                // @mago-ignore analysis:mixed-assignment
                $rightValue = $rights[$i];

                if (!(is_int($leftValue) || is_float($leftValue)) || !(is_int($rightValue) || is_float($rightValue))) {
                    throw new InvalidArgumentException('Minus function requires non-null values');
                }

                $result = $leftValue - $rightValue;

                if (is_int($leftValue) && is_int($rightValue) && is_float($result)) {
                    throw new InvalidArgumentException('Minus function integer overflow');
                }

                $results[] = $result;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return $this->arithmeticResult($left, $right, $results);
    }

    public function multiply(Column $left, Column $right): Column
    {
        $rights = $right->physicals();
        $results = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($left->physicals() as $i => $leftValue) {
                // @mago-ignore analysis:mixed-assignment
                $rightValue = $rights[$i];

                if (!(is_int($leftValue) || is_float($leftValue)) || !(is_int($rightValue) || is_float($rightValue))) {
                    throw new InvalidArgumentException('Multiply function requires non-null values');
                }

                $result = $leftValue * $rightValue;

                if (is_int($leftValue) && is_int($rightValue) && is_float($result)) {
                    throw new InvalidArgumentException('Multiply function integer overflow');
                }

                $results[] = $result;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return $this->arithmeticResult($left, $right, $results);
    }

    public function modulo(Column $left, Column $right): Column
    {
        $calculator = new Calculator();
        $rights = $right->physicals();
        $results = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($left->physicals() as $i => $leftValue) {
                // @mago-ignore analysis:mixed-assignment
                $rightValue = $rights[$i];

                if (!is_int($leftValue) || !is_int($rightValue)) {
                    throw new InvalidArgumentException('Mod function requires non-null values');
                }

                if ($rightValue === 0) {
                    throw new InvalidArgumentException('Mod function cannot perform modulo by zero');
                }

                if ($rightValue < 0) {
                    $results[] = $calculator->modulus($leftValue, $rightValue);

                    continue;
                }

                // Calculator::modulus's int path inlined: the method call costs as much as the arithmetic
                $remainder = $leftValue % $rightValue;
                $results[] = $remainder < 0 ? $remainder + $rightValue : $remainder;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        $definition = Definitions::of('value', type_integer(), true);

        return new ScalarColumn($definition->type(), (new PhysicalFor())->definition($definition), $results, 0);
    }

    public function sort(Rows $rows, array $columns): Rows
    {
        $order = new RowOrder($columns);

        return $rows->gather($order->permutation($order->keys($rows), $rows->count()));
    }

    public function sortWorkingBytes(Rows $rows, array $columns): int
    {
        $bytes = 0;

        foreach ($rows->columns() as $column) {
            $bytes += $this->gatheredBytes($column);
        }

        foreach ($columns as $sortColumn) {
            $bytes += $this->orderedBytes($rows->column($sortColumn->name));
        }

        return $bytes;
    }

    /**
     * Bytes take() allocates for a copy of $column, at its peak.
     */
    public function gatheredBytes(Column $column): int
    {
        // measured per slot, doubled as arrays grow in powers of two; nested columns copy their children too
        return match (true) {
            $column instanceof ConstantColumn => 0,
            $column instanceof ListColumn => (48 * $column->count())
                + (48 * $column->element->count())
                + $this->gatheredBytes($column->element),
            $column instanceof MapColumn => (48 * $column->count())
                + (48 * $column->keys->count())
                + $this->gatheredBytes($column->keys)
                + $this->gatheredBytes($column->values),
            $column instanceof StructColumn => (16 * $column->count())
                + array_sum(array_map($this->gatheredBytes(...), $column->children)),
            default => ($column->nullCount() === 0 ? 40 : 48) * $column->count(),
        };
    }

    /**
     * Bytes RowOrder allocates to sort by $column: its key and its share of the permutation, at their peak.
     */
    public function orderedBytes(Column $column): int
    {
        $flag = RowOrder::flag($column->type());
        $nested = static fn(Column $child): bool => (
            $child instanceof ListColumn
            || $child instanceof MapColumn
            || $child instanceof StructColumn
        );
        // RowOrder's fast path builds no rank array; a float may hold a NaN, which takes the rank path
        $perRow =
            $column->nullCount() === 0
            && ($flag === SORT_STRING || $flag === SORT_NUMERIC && !type_bare($column->type()) instanceof FloatType)
                ? 224
                : 320;

        return ($perRow * $column->count())
        + match (true) {
            (new ComparableValues())->equalByPhysical($column->type()) => 0,
            // values() builds a PHP array per row: packed (8 slots at least) for a list, hashed for a map or a structure
            $column instanceof ListColumn => (192 * $column->count())
                + (32 * $column->element->count())
                + ($nested($column->element) ? $this->orderedBytes($column->element) : 0),
            $column instanceof MapColumn => (448 * $column->count())
                + (64 * $column->keys->count())
                + ($nested($column->values) ? $this->orderedBytes($column->values) : 0),
            $column instanceof StructColumn => ((448 + (64 * count($column->children))) * $column->count())
                + array_sum(array_map(fn(Column $child): int => $nested($child)
                    ? $this->orderedBytes($child)
                    : 0, $column->children)),
            default => 448 * $column->count(),
        };
    }

    public function groupedTotals(): GroupedTotals
    {
        return new PhpGroupedTotals();
    }

    public function groupedCounts(): GroupedCounts
    {
        return new PhpGroupedCounts();
    }

    public function groupTable(array $columns, Schema $declared): GroupTable
    {
        return new PhpGroupTable($columns, $declared);
    }

    public function adopt(Definition $definition, Column $column): Column
    {
        if ($column instanceof ValueColumn) {
            throw ColumnMismatchException::untypedColumn($definition->name());
        }

        if (
            $column instanceof ScalarColumn
            || $column instanceof ConstantColumn
            || $column instanceof ListColumn
            || $column instanceof MapColumn
            || $column instanceof StructColumn
        ) {
            return $column;
        }

        $builder = $this->builder($definition);

        if ($column->count() > 0) {
            $builder->appendTake($column, range(0, $column->count() - 1));
        }

        return $builder->finish();
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

    /**
     * @param list<mixed> $results
     */
    private function arithmeticResult(Column $left, Column $right, array $results): Column
    {
        $definition = Definitions::of(
            'value',
            type_bare($left->type()) instanceof IntegerType && type_bare($right->type()) instanceof IntegerType
                ? type_integer()
                : type_float(),
            true,
        );

        return new ScalarColumn($definition->type(), (new PhysicalFor())->definition($definition), $results, 0);
    }

    private function nullTest(Column $column, bool $isNull): Column
    {
        if ($column->nullCount() === 0) {
            return $this->constant(Definitions::of('value', type_boolean(), true), !$isNull, $column->count());
        }

        $results = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($column->physicals() as $physical) {
            $results[] = ($physical === null) === $isNull;
        }

        $definition = Definitions::of('value', type_boolean(), true);

        return new ScalarColumn($definition->type(), (new PhysicalFor())->definition($definition), $results, 0);
    }
}
