<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use Flow\ETL\FlowPhpExtension;
use Flow\ETL\Schema\Definition;

final readonly class AdaptiveBackend implements Backend
{
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

    public function adopt(Definition $definition, Column $column): Column
    {
        return $this->backend->adopt($definition, $column);
    }

    public function allocatedBytes(): int
    {
        return $this->backend->allocatedBytes();
    }
}
