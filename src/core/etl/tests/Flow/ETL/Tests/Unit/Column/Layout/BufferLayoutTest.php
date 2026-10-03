<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Layout;

use Flow\ETL\Column\Layout\BufferLayout;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Mother\ColumnMother;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;
use function pack;

final class BufferLayoutTest extends TestCase
{
    /**
     * @return Generator<string, array{Type<mixed>, int, int}>
     */
    public static function counts(): Generator
    {
        yield 'int' => [type_integer(), 1, 2];
        yield 'float' => [type_float(), 1, 2];
        yield 'date' => [type_date(), 1, 2];
        yield 'bool' => [type_boolean(), 1, 2];
        yield 'string' => [type_string(), 1, 3];
        yield 'uuid' => [type_uuid(), 1, 2];
        yield 'null' => [type_null(), 1, 0];
        yield 'optional' => [type_optional(type_string()), 1, 3];
        yield 'list' => [type_list(type_string()), 2, 5];
        yield 'map' => [type_map(type_string(), type_integer()), 4, 8];
        yield 'structure' => [type_structure(['a' => type_integer(), 'b' => type_list(type_null())]), 4, 5];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('counts')]
    public function test_counts_nodes_and_buffers(Type $type, int $nodes, int $buffers): void
    {
        static::assertSame($nodes, (new BufferLayout())->nodeCount($type));
        static::assertSame($buffers, (new BufferLayout())->bufferCount($type));
    }

    public function test_the_first_node_is_given(): void
    {
        static::assertSame([[3, 3]], (new BufferLayout())->nodes(type_null(), 3, 3, []));
        static::assertSame([[2, 1]], (new BufferLayout())->nodes(type_integer(), 2, 1, ['', '']));
    }

    public function test_a_list_child_spans_the_last_offset(): void
    {
        $type = type_list(type_optional(type_integer()));
        $column = ColumnMother::of(list_schema('a', $type, nullable: true), [[1, null], null, [2, 3, null]]);

        static::assertSame([[3, 1], [5, 2]], (new BufferLayout())->nodes($column->type(), 3, 1, $column->encode()));
    }

    public function test_a_map_entries_node_holds_no_nulls(): void
    {
        $type = type_map(type_string(), type_optional(type_integer()));
        $column = ColumnMother::of(map_schema('a', $type), [['a' => 1, 'b' => null], ['c' => null]]);

        static::assertSame(
            [[2, 0], [3, 0], [3, 0], [3, 2]],
            (new BufferLayout())->nodes($type, 2, 0, $column->encode()),
        );
    }

    public function test_structure_children_span_their_parent(): void
    {
        $type = type_structure(['a' => type_integer(), 'n' => type_optional(type_null())]);
        $column = ColumnMother::of(structure_schema('s', $type, nullable: true), [['a' => 1, 'n' => null], null]);

        static::assertSame(
            [[2, 1], [2, 1], [2, 2]],
            (new BufferLayout())->nodes($column->type(), 2, 1, $column->encode()),
        );
    }

    public function test_refuses_a_buffer_count_other_than_the_layout(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column of type integer holds 1 buffers, its layout needs 2');

        (new BufferLayout())->nodes(type_integer(), 1, 0, ['']);
    }

    public function test_refuses_an_offsets_buffer_of_another_length(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('List offsets buffer of 4 bytes, expected 8 for 1 rows');

        (new BufferLayout())->nodes(type_list(type_integer()), 1, 0, ['', pack('V', 0), '', '']);
    }

    public function test_refuses_a_map_entries_validity(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Map entries validity must be omitted, got 1 bytes');

        (new BufferLayout())->nodes(type_map(type_string(), type_integer()), 1, 0, [
            '',
            pack('V2', 0, 0),
            "\x01",
            '',
            pack('V', 0),
            '',
            '',
            '',
        ]);
    }

    public function test_refuses_a_short_child_validity(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Validity bitmap of 1 bytes is too short for 9 rows');

        (new BufferLayout())->nodes(type_list(type_optional(type_integer())), 1, 0, ['', pack('V2', 0, 9), "\x01", '']);
    }
}
