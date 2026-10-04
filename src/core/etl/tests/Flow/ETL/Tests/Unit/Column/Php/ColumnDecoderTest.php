<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use Flow\ETL\Column\Layout\Buffers;
use Flow\ETL\Column\Php\ColumnDecoder;
use Flow\ETL\Column\Php\ConstantColumn;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Mother\ColumnMother;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function pack;

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
    }

    public function test_refuses_a_null_count_other_than_the_bitmap(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column null count 1 disagrees with its validity bitmap (0 nulls in 1 rows)');

        (new ColumnDecoder())->decode(type_integer(), new Buffers(['', "\0\0\0\0\0\0\0\0"]), 1, 1);
    }

    public function test_refuses_nulls_in_a_non_optional_list_element(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('List child holds 1 nulls in a non-nullable integer');

        (new ColumnDecoder())->decode(
            type_list(type_integer()),
            new Buffers(['', pack('V2', 0, 1), "\x00", "\0\0\0\0\0\0\0\0"]),
            1,
            0,
        );
    }

    public function test_refuses_nulls_in_a_map_key(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Map key child holds 1 nulls in a non-nullable string');

        (new ColumnDecoder())->decode(
            type_map(type_string(), type_integer()),
            new Buffers(['', pack('V2', 0, 1), '', "\x00", pack('V2', 0, 0), '', '', "\0\0\0\0\0\0\0\0"]),
            1,
            0,
        );
    }

    public function test_refuses_nulls_in_a_non_optional_map_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Map value child holds 1 nulls in a non-nullable integer');

        (new ColumnDecoder())->decode(
            type_map(type_string(), type_integer()),
            new Buffers(['', pack('V2', 0, 1), '', '', pack('V2', 0, 1), 'k', "\x00", "\0\0\0\0\0\0\0\0"]),
            1,
            0,
        );
    }

    public function test_null_kind_children_hold_nulls(): void
    {
        $list = type_list(type_null());
        $map = type_map(type_string(), type_null());

        static::assertSame(
            [[null, null]],
            (new ColumnDecoder())
                ->decode($list, new Buffers(ColumnMother::of(list_schema('a', $list), [[null, null]])->encode()), 1, 0)
                ->values(),
        );
        static::assertSame(
            [['k' => null]],
            (new ColumnDecoder())
                ->decode($map, new Buffers(ColumnMother::of(map_schema('a', $map), [['k' => null]])->encode()), 1, 0)
                ->values(),
        );
    }

    public function test_a_structure_child_under_a_null_parent_holds_nulls(): void
    {
        $type = type_structure(['a' => type_integer()]);
        $column = ColumnMother::of(structure_schema('s', $type, nullable: true), [['a' => 1], null]);

        static::assertSame(
            [['a' => 1], null],
            (new ColumnDecoder())
                ->decode($column->type(), new Buffers($column->encode()), 2, 1)
                ->values(),
        );
    }
}
