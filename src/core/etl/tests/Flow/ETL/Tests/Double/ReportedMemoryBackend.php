<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Column\Backend;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\ColumnBuilder;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Schema\Definition;

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
}
