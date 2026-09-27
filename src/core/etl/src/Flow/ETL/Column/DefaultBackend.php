<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use Flow\ETL\Schema\Definition;

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
}
