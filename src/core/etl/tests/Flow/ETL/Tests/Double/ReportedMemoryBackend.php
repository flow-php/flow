<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\ColumnBuilder;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;

/**
 * A PHP backend whose native memory is whatever the test reports - far above PHP's own allocations, so a memory limit
 * decides deterministically.
 */
final class ReportedMemoryBackend implements Backend
{
    public function __construct(
        public int $reported = 0,
        public readonly PhpBackend $php = new PhpBackend(),
    ) {}

    public function adopt(Definition $definition, Column $column): Column
    {
        return $this->php->adopt($definition, $column);
    }

    public function allocatedBytes(): int
    {
        return $this->reported;
    }

    public function builder(Definition $definition): ColumnBuilder
    {
        return $this->php->builder($definition);
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
