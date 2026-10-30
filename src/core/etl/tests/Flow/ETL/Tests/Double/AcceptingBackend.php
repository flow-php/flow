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

    public function decodeFrame(string $body, Schema $schema, Codec $codec): Rows
    {
        return (new PhpBackend())->decodeFrame($body, $schema, $codec);
    }

    public function encodeFrame(Rows $rows, Codec $codec): string
    {
        return (new PhpBackend())->encodeFrame($rows, $codec);
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
