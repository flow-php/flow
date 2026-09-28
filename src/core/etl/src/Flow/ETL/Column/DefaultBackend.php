<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use Flow\ETL\Schema\Definition;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final readonly class DefaultBackend implements Backend
{
    private PhpBackend $php;

    public function __construct()
    {
        $this->php = new PhpBackend();
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

    public function adopt(Definition $definition, Column $column): Column
    {
        return $this->php->adopt($definition, $column);
    }

    public function allocatedBytes(): int
    {
        return $this->php->allocatedBytes();
    }
}
