<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\ColumnMismatchException;
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
use Flow\ETL\Values\Column\Php\ScalarColumn;
use Flow\ETL\Values\Column\Php\StructColumn;
use Flow\ETL\Values\Column\Physical\PhysicalFor;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definition\NullDefinition;
use SensitiveParameter;

use function array_sum;
use function array_values;
use function intdiv;
use function pack;
use function range;
use function sprintf;
use function str_repeat;
use function strlen;
use function substr;
use function unpack;

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
}
