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

final class SpyBackend implements Backend
{
    private int $adopts = 0;

    private int $builders = 0;

    private int $constants = 0;

    private int $decodes = 0;

    private int $decodeFrames = 0;

    private int $encodeFrames = 0;

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

    public function decodeFrame(string $body, Schema $schema, Codec $codec): Rows
    {
        $this->decodeFrames++;

        return $this->php->decodeFrame($body, $schema, $codec);
    }

    public function decodeFrames(): int
    {
        return $this->decodeFrames;
    }

    public function encodeFrame(Rows $rows, Codec $codec): string
    {
        $this->encodeFrames++;

        return $this->php->encodeFrame($rows, $codec);
    }

    public function encodeFrames(): int
    {
        return $this->encodeFrames;
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
