<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Column\Backend;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\ColumnBuilder;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Schema\Definition;

final class SpyBackend implements Backend
{
    private int $builders = 0;

    private readonly PhpBackend $php;

    public function __construct()
    {
        $this->php = new PhpBackend();
    }

    public function builder(Definition $definition): ColumnBuilder
    {
        $this->builders++;

        return $this->php->builder($definition);
    }

    public function builders(): int
    {
        return $this->builders;
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
