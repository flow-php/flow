<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\DataFrame;
use Flow\ETL\Function\ArrayExpand\ArrayExpand;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_expand;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure;
use function Flow\ETL\DSL\structure_get;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\to_memory;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ArrayExpandTest extends FlowTestCase
{
    public function test_expand_nested_in_a_structure_gives_one_row_per_element(): void
    {
        $frame = static fn(): DataFrame => data_frame()
            ->read(from_array(
                [['id' => 1, 'tags' => ['a', 'b']], ['id' => 2, 'tags' => []]],
                schema(int_schema('id'), list_schema('tags', type_list(type_string()))),
            ))
            ->withEntry('tag', structure(['name' => ref('tags')->expand()]))
            ->drop('tags');

        static::assertSame('structure{name: string}', $frame()->schema()->get('tag')->type()->toString());
        static::assertSame(
            [
                ['id' => 1, 'tag' => ['name' => 'a']],
                ['id' => 1, 'tag' => ['name' => 'b']],
            ],
            $frame()->fetch()->toArray(),
        );
    }

    public function test_expands_in_one_expression_are_zipped_and_padded_with_null(): void
    {
        $frame = static fn(): DataFrame => data_frame()
            ->read(from_array(
                [
                    ['id' => 1, 'tags' => ['a', 'b'], 'scores' => [10, 20, 30]],
                    ['id' => 2, 'tags' => [], 'scores' => [40]],
                ],
                schema(
                    int_schema('id'),
                    list_schema('tags', type_list(type_string())),
                    list_schema('scores', type_list(type_integer())),
                ),
            ))
            ->withEntry('pair', structure(['tag' => ref('tags')->expand(), 'score' => ref('scores')->expand()]))
            ->drop('tags', 'scores');

        static::assertSame(
            'structure{tag: ?string, score: ?integer}',
            $frame()->schema()->get('pair')->type()->toString(),
        );
        static::assertSame(
            [
                ['id' => 1, 'pair' => ['tag' => 'a', 'score' => 10]],
                ['id' => 1, 'pair' => ['tag' => 'b', 'score' => 20]],
                ['id' => 1, 'pair' => ['tag' => null, 'score' => 30]],
                ['id' => 2, 'pair' => ['tag' => null, 'score' => 40]],
            ],
            $frame()->fetch()->toArray(),
        );
    }

    public function test_expand_both(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
            ]))
            ->withEntry('expanded', array_expand(ref('array'), ArrayExpand::BOTH))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'expanded' => ['a' => 1]],
                ['id' => 1, 'expanded' => ['b' => 2]],
                ['id' => 1, 'expanded' => ['c' => 3]],
            ],
            $memory->dump(),
        );
    }

    public function test_expand_keys(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
            ]))
            ->withEntry('expanded', array_expand(ref('array'), ArrayExpand::KEYS))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'expanded' => 'a'],
                ['id' => 1, 'expanded' => 'b'],
                ['id' => 1, 'expanded' => 'c'],
            ],
            $memory->dump(),
        );
    }

    public function test_expand_values(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
            ]))
            ->withEntry('expanded', array_expand(ref('array')))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'expanded' => 1],
                ['id' => 1, 'expanded' => 2],
                ['id' => 1, 'expanded' => 3],
            ],
            $memory->dump(),
        );
    }

    public function test_expand_of_an_absent_optional_collection_gives_no_rows(): void
    {
        data_frame()
            ->read(from_array(
                [['id' => 1, 'b' => ['x' => [1, 2]]], ['id' => 2, 'b' => []]],
                schema(
                    int_schema('id'),
                    structure_schema('b', type_structure([
                        'x' => structure_element('x', type_list(type_integer()), optional: true),
                    ])),
                ),
            ))
            ->withEntry('item', array_expand(structure_get(ref('b'), '?x')))
            ->drop('b')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame([['id' => 1, 'item' => 1], ['id' => 1, 'item' => 2]], $memory->dump());
    }

    public function test_nested_expand_of_a_null_list_gives_no_rows(): void
    {
        data_frame()
            ->read(from_array(
                [['id' => 1, 'data' => [1, 2]], ['id' => 2, 'data' => null]],
                schema(int_schema('id'), list_schema('data', type_list(type_integer()), nullable: true)),
            ))
            ->withEntry('item', optional(array_expand(ref('data'))))
            ->drop('data')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame([['id' => 1, 'item' => 1], ['id' => 1, 'item' => 2]], $memory->dump());
    }
}
