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
    private int $adopts = 0;

    private int $builders = 0;

    private int $constants = 0;

    private int $decodes = 0;

    private readonly PhpBackend $php;

    public function __construct(
        private readonly int $allocatedBytes = 0,
    ) {
        $this->php = new PhpBackend();
    }

    public function adopt(Definition $definition, Column $column): Column
    {
        $this->adopts++;

        return $this->php->adopt($definition, $column);
    }

    public function adopts(): int
    {
        return $this->adopts;
    }

    public function allocatedBytes(): int
    {
        return $this->allocatedBytes;
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

    public function decodes(): int
    {
        return $this->decodes;
    }

    public function constant(Definition $definition, mixed $value, int $count): Column
    {
        $this->constants++;

        return $this->php->constant($definition, $value, $count);
    }

    public function constants(): int
    {
        return $this->constants;
    }

    public function decode(Definition $definition, array $buffers, int $count, int $nullCount): Column
    {
        $this->decodes++;

        return $this->php->decode($definition, $buffers, $count, $nullCount);
    }
}
