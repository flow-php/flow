<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use Flow\ETL\Schema\Definition;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class RustBackend implements Backend
{
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

    public function constant(Definition $definition, mixed $value, int $count): Column
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function decode(Definition $definition, array $buffers, int $count, int $nullCount): Column
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
