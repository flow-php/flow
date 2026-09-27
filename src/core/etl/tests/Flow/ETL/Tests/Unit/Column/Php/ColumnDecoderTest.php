<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use Flow\ETL\Column\Php\Buffers;
use Flow\ETL\Column\Php\ColumnDecoder;
use Flow\ETL\Column\Php\ConstantColumn;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Mother\ColumnMother;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_structure;

final class ColumnDecoderTest extends TestCase
{
    public function test_derives_child_null_counts(): void
    {
        $type = type_list(type_optional(type_integer()));
        $column = ColumnMother::of(list_schema('a', $type), [[1, null], [null]]);
        $decoded = (new ColumnDecoder())->decode($type, new Buffers($column->encode()), 2, 0);

        static::assertSame([[1, null], [null]], $decoded->values());
    }

    public function test_a_null_kind_consumes_no_buffer(): void
    {
        $type = type_structure(['n' => type_optional(type_null()), 'a' => type_integer()]);
        $column = ColumnMother::of(structure_schema('s', $type), [['n' => null, 'a' => 1]]);
        $buffers = new Buffers($column->encode());
        $decoded = (new ColumnDecoder())->decode($type, $buffers, 1, 0);

        static::assertSame(0, $buffers->remaining());
        static::assertSame([['n' => null, 'a' => 1]], $decoded->values());
    }

    public function test_a_top_level_null_column(): void
    {
        static::assertInstanceOf(ConstantColumn::class, (new ColumnDecoder())->decode(
            type_null(),
            new Buffers([]),
            3,
            3,
        ));
        static::assertInstanceOf(ConstantColumn::class, (new ColumnDecoder())->decode(
            type_null(),
            new Buffers([]),
            3,
            -1,
        ));
    }

    public function test_refuses_a_null_count_other_than_the_bitmap(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column null count 1 disagrees with its validity bitmap (0 nulls in 1 rows)');

        (new ColumnDecoder())->decode(type_integer(), new Buffers(['', "\0\0\0\0\0\0\0\0"]), 1, 1);
    }
}
