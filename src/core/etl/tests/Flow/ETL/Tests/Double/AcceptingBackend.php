<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Column\Backend;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\ColumnBuilder;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Schema\Definition;

/**
 * A backend whose builders take any value, so a replay through it finds no violation.
 */
final readonly class AcceptingBackend implements Backend
{
    public function builder(Definition $definition): ColumnBuilder
    {
        return new AcceptingColumnBuilder();
    }

    public function constant(Definition $definition, mixed $value, int $count): Column
    {
        return (new PhpBackend())->constant($definition, $value, $count);
    }

    public function decode(Definition $definition, array $buffers, int $count, int $nullCount): Column
    {
        return (new PhpBackend())->decode($definition, $buffers, $count, $nullCount);
    }

    public function adopt(Definition $definition, Column $column): Column
    {
        return $column;
    }

    public function allocatedBytes(): int
    {
        return 0;
    }
}
