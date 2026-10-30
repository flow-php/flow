<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Functions;

use Flow\ETL\API\DataFrame;
use Flow\ETL\Functions\ArrayExpand\ArrayExpand;
use Flow\ETL\Operators\Processor\ExpandingProcessor;
use Flow\ETL\Planning\Logical\Explain;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\Context\PipelineStepList;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ExpansionMother;
use Flow\ETL\Values\Rows;

use function Flow\ETL\DSL\array_expand;
use function Flow\ETL\DSL\array_get;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
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
    public function test_a_page_of_25_000_structures_expanded_into_a_new_column(): void
    {
        static::assertSame(
            25_000,
            data_frame()
                ->read(from_array([['body' => ExpansionMother::page(25_000)]], schema(str_schema('body'))))
                ->withColumn(
                    'body',
                    ref('body')
                        ->jsonDecode()
                        ->cast(type_structure([
                            'rows' => type_list(ExpansionMother::pageRowType()),
                        ])),
                )
                ->withColumn('record', array_expand(array_get(ref('body'), 'rows')))
                ->select('record')
                ->fetch()
                ->count(),
        );
    }

    public function test_a_page_of_25_000_structures_expanded_in_place(): void
    {
        static::assertSame(
            25_000,
            data_frame()
                ->read(from_array([['body' => ExpansionMother::page(25_000)]], schema(str_schema('body'))))
                ->withColumn(
                    'body',
                    ref('body')
                        ->jsonDecode()
                        ->cast(type_structure([
                            'rows' => type_list(ExpansionMother::pageRowType()),
                        ])),
                )
                ->withColumn('body', array_expand(array_get(ref('body'), 'rows')))
                ->select('body')
                ->fetch()
                ->count(),
        );
    }

    public function test_100_000_short_strings_expanded_into_a_new_column(): void
    {
        static::assertSame(
            100_000,
            data_frame()
                ->read(from_array([[
                    'items' => ExpansionMother::terms(100_000),
                ]], schema(list_schema('items', type_list(type_string())))))
                ->withColumn('item', array_expand(ref('items')))
                ->select('item')
                ->fetch()
                ->count(),
        );
    }

    public function test_100_000_short_strings_expanded_in_place(): void
    {
        static::assertSame(
            100_000,
            data_frame()
                ->read(from_array([[
                    'items' => ExpansionMother::terms(100_000),
                ]], schema(list_schema('items', type_list(type_string())))))
                ->withColumn('items', array_expand(ref('items')))
                ->select('items')
                ->fetch()
                ->count(),
        );
    }

    public function test_a_small_input_column_is_copied_to_every_expanded_row(): void
    {
        $rows = data_frame()
            ->read(from_array(
                [['id' => 7, 'items' => ExpansionMother::terms(100_000)]],
                schema(int_schema('id'), list_schema('items', type_list(type_string()))),
            ))
            ->withColumn('item', array_expand(ref('items')))
            ->select('id', 'item')
            ->fetch();

        static::assertSame(100_000, $rows->count());
        static::assertSame(['id' => 7, 'item' => 'term-1'], $rows->toArray()[0]);
        static::assertSame(['id' => 7, 'item' => 'term-100000'], $rows->toArray()[99_999]);
    }

    public function test_a_nested_expand_over_a_page_of_25_000_structures(): void
    {
        static::assertSame(
            25_000,
            data_frame()
                ->read(from_array([['body' => ExpansionMother::page(25_000)]], schema(str_schema('body'))))
                ->withColumn(
                    'body',
                    ref('body')
                        ->jsonDecode()
                        ->cast(type_structure([
                            'rows' => type_list(ExpansionMother::pageRowType()),
                        ])),
                )
                ->withColumn('record', array_get(array_expand(array_get(ref('body'), 'rows')), 'clicks'))
                ->select('record')
                ->fetch()
                ->count(),
        );
    }

    public function test_an_unpack_over_a_page_of_25_000_expanded_structures(): void
    {
        static::assertSame(
            25_000,
            data_frame()
                ->read(from_array([['body' => ExpansionMother::page(25_000)]], schema(str_schema('body'))))
                ->withColumn(
                    'body',
                    ref('body')
                        ->jsonDecode()
                        ->cast(type_structure([
                            'rows' => type_list(ExpansionMother::pageRowType()),
                        ])),
                )
                ->withColumn(
                    'record',
                    array_expand(array_get(ref('body'), 'rows'))->unpack(schema(int_schema('clicks'))),
                )
                ->select('record.clicks')
                ->fetch()
                ->count(),
        );
    }

    public function test_an_expansion_emits_batches_of_at_most_1_000_rows(): void
    {
        static::assertSame(
            [1000, 1000, 500],
            array_map(
                static fn(Rows $rows): int => $rows->count(),
                iterator_to_array(
                    data_frame()
                        ->read(from_array([[
                            'items' => ExpansionMother::terms(2_500),
                        ]], schema(list_schema('items', type_list(type_string())))))
                        ->withColumn('item', array_expand(ref('items')))
                        ->get(),
                    false,
                ),
            ),
        );
    }

    public function test_an_expanding_with_column_runs_on_the_expanding_processor(): void
    {
        $steps = static function (DataFrame $frame): array {
            $plan = $frame->explain();

            return PipelineStepList::classes($plan->planner->plan($plan->logical)->root());
        };
        $frame = static fn(): DataFrame => data_frame()->read(from_array([['items' => [
            'a',
            'b',
        ]]], schema(list_schema('items', type_list(type_string())))));

        static::assertContains(
            ExpandingProcessor::class,
            $steps($frame()->withColumn('item', array_expand(ref('items')))),
        );
        static::assertNotContains(ExpandingProcessor::class, $steps($frame()->withColumn('item', lit(1))));
    }

    public function test_explain_shows_the_columns_an_expand_keeps(): void
    {
        $plan = data_frame()
            ->read(from_array([[
                'items' => ExpansionMother::terms(3),
            ]], schema(list_schema('items', type_list(type_string())))))
            ->withColumn('item', array_expand(ref('items')))
            ->select('item')
            ->explain();

        static::assertStringContainsString(
            'Keeps: item',
            (new Explain())->of($plan->planner->optimize($plan->logical)),
        );
    }

    public function test_expand_nested_in_a_structure_gives_one_row_per_element(): void
    {
        $frame = static fn(): DataFrame => data_frame()
            ->read(from_array(
                [['id' => 1, 'tags' => ['a', 'b']], ['id' => 2, 'tags' => []]],
                schema(int_schema('id'), list_schema('tags', type_list(type_string()))),
            ))
            ->withColumn('tag', structure(['name' => ref('tags')->expand()]))
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
            ->withColumn('pair', structure(['tag' => ref('tags')->expand(), 'score' => ref('scores')->expand()]))
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
            ->withColumn('expanded', array_expand(ref('array'), ArrayExpand::BOTH))
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
            ->withColumn('expanded', array_expand(ref('array'), ArrayExpand::KEYS))
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
            ->withColumn('expanded', array_expand(ref('array')))
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
            ->withColumn('item', array_expand(structure_get(ref('b'), '?x')))
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
            ->withColumn('item', optional(array_expand(ref('data'))))
            ->drop('data')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame([['id' => 1, 'item' => 1], ['id' => 1, 'item' => 2]], $memory->dump());
    }
}
