<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Function\ArrayExpand;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Plan\RequiredColumns;
use Flow\ETL\Processor\ExpandingProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\ExpandingProcessorContext;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ListColumnsMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\when;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ExpandingProcessorTest extends FlowTestCase
{
    public function test_a_batch_size_below_one_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, given: 0');

        new ExpandingProcessor('v', ref('tags')->expand(), RequiredColumns::all(), 0);
    }

    public function test_expand_results(): void
    {
        static::assertSame(
            [['array' => 1], ['array' => 2], ['array' => 3]],
            ExpandingProcessorContext::rows(
                flow_context(config()),
                new ExpandingProcessor(
                    'array',
                    new ArrayExpand(lit([1, 2, 3]), ArrayExpand\ArrayExpand::VALUES),
                    RequiredColumns::all(),
                ),
                array_to_rows([[]], schema()),
            ),
        );
    }

    public function test_output_is_split_into_batches_of_at_most_the_batch_size(): void
    {
        static::assertSame(
            [2, 2, 1],
            array_map(
                static fn(Rows $rows): int => $rows->count(),
                ExpandingProcessorContext::batches(
                    flow_context(config()),
                    new ExpandingProcessor('tag', ref('tags')->expand(), RequiredColumns::all(), 2),
                    array_to_rows([[
                        'id' => 'a',
                        'tags' => ['v', 'w', 'x', 'y', 'z'],
                    ]], ListColumnsMother::tagsSchema()),
                ),
            ),
        );
    }

    public function test_bind_declares_the_output_of_the_bound_expansion(): void
    {
        static::assertEquals(
            schema(str_schema('id'), str_schema('tag')),
            (new ExpandingProcessor('tag', ref('tags')->expand(), RequiredColumns::allBut('tags')))->bind(
                ListColumnsMother::tagsSchema(),
            )->output,
        );
    }

    public function test_bound_and_unbound_give_the_same_rows(): void
    {
        $processor = new ExpandingProcessor('tag', ref('tags')->expand(), RequiredColumns::only('id', 'tag'));
        $rows = array_to_rows([['id' => 'a', 'tags' => ['x', 'y']]], ListColumnsMother::tagsSchema());

        static::assertSame(
            [['id' => 'a', 'tag' => 'x'], ['id' => 'a', 'tag' => 'y']],
            ExpandingProcessorContext::rows(flow_context(config()), $processor, $rows),
        );
        static::assertSame(
            ExpandingProcessorContext::rows(flow_context(config()), $processor, $rows),
            ExpandingProcessorContext::rows(
                flow_context(config()),
                $processor->bind(ListColumnsMother::tagsSchema())->step,
                $rows,
            ),
        );
    }

    public function test_stop_after_the_first_batch_is_forwarded_upstream(): void
    {
        $upstream = (new CountingExtractor(ListColumnsMother::tagsSchema(), array_to_rows([
            ['id' => 'a', 'tags' => ['x', 'y']],
            ['id' => 'b', 'tags' => ['z']],
        ], ListColumnsMother::tagsSchema())))
            ->withBatchSize(1)
            ->extract(flow_context());
        $processed = (new ExpandingProcessor('tag', ref('tags')->expand(), RequiredColumns::all(), 1))->process(
            $upstream,
            flow_context(),
        );

        static::assertTrue($processed->valid());

        $processed->send(Signal::STOP);

        static::assertFalse($processed->valid());
        static::assertFalse($upstream->valid());
    }

    public function test_an_expanded_null_under_a_not_null_declaration_names_its_row(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "out" (row 1)');

        ExpandingProcessorContext::rows(
            flow_context(config()),
            new ExpandingProcessor(
                int_schema('out'),
                new ArrayExpand(lit([1, null]), ArrayExpand\ArrayExpand::VALUES),
                RequiredColumns::all(),
            ),
            array_to_rows([[]], schema()),
        );
    }

    public function test_bind_declares_zipped_expands_nullable(): void
    {
        static::assertEquals(
            schema(
                str_schema('id'),
                list_schema('tags', type_list(type_string())),
                list_schema('nums', type_list(type_integer())),
                structure_schema('s', type_structure([
                    'n' => type_optional(type_integer()),
                    'tag' => type_optional(type_string()),
                ])),
            ),
            (new ExpandingProcessor(
                's',
                structure([
                    'n' => ref('nums')->expand(),
                    'tag' => ref('tags')->expand(),
                ]),
                RequiredColumns::all(),
            ))->bind(schema(
                str_schema('id'),
                list_schema('tags', type_list(type_string())),
                list_schema('nums', type_list(type_integer())),
            ))->output,
        );
    }

    public function test_an_unbound_nested_expand_is_expanded(): void
    {
        static::assertSame(
            [
                ['id' => 'a', 'tags' => ['x', 'y'], 's' => ['tag' => 'x']],
                ['id' => 'a', 'tags' => ['x', 'y'], 's' => ['tag' => 'y']],
            ],
            ExpandingProcessorContext::rows(
                flow_context(config()),
                new ExpandingProcessor('s', structure(['tag' => ref('tags')->expand()]), RequiredColumns::all()),
                array_to_rows([['id' => 'a', 'tags' => ['x', 'y']]], ListColumnsMother::tagsSchema()),
            ),
        );
    }

    public function test_an_unbound_unpack_over_a_nested_expand_is_expanded(): void
    {
        static::assertSame(
            [
                ['id' => 'a', 'tags' => ['x', 'y'], 'u.tag' => 'x'],
                ['id' => 'a', 'tags' => ['x', 'y'], 'u.tag' => 'y'],
            ],
            ExpandingProcessorContext::rows(
                flow_context(config()),
                new ExpandingProcessor(
                    'u',
                    structure(['tag' => ref('tags')->expand()])->unpack(schema(str_schema('tag'))),
                    RequiredColumns::all(),
                ),
                array_to_rows([['id' => 'a', 'tags' => ['x', 'y']]], ListColumnsMother::tagsSchema()),
            ),
        );
    }

    public function test_an_expand_explodes_every_row_of_the_batch_in_order(): void
    {
        static::assertSame(
            [['id' => 1, 'value' => 1], ['id' => 1, 'value' => 2], ['id' => 3, 'value' => 3]],
            ExpandingProcessorContext::rows(
                flow_context(config()),
                new ExpandingProcessor('value', ref('list')->expand(), RequiredColumns::only('id', 'value')),
                array_to_rows(
                    [['id' => 1, 'list' => [1, 2]], ['id' => 2, 'list' => []], ['id' => 3, 'list' => [3]]],
                    schema(int_schema('id'), list_schema('list', type_list(type_integer()))),
                ),
            ),
        );
    }

    public function test_emits_one_row_per_element_and_none_for_an_empty_list(): void
    {
        static::assertSame(
            [
                ['id' => 'a', 'tags' => ['x', 'y'], 's' => ['tag' => 'x']],
                ['id' => 'a', 'tags' => ['x', 'y'], 's' => ['tag' => 'y']],
            ],
            ExpandingProcessorContext::rows(
                flow_context(config()),
                (new ExpandingProcessor(
                    's',
                    structure(['tag' => ref('tags')->expand()]),
                    RequiredColumns::all(),
                ))->bind(ListColumnsMother::tagsSchema())->step,
                array_to_rows([
                    ['id' => 'a', 'tags' => ['x', 'y']],
                    ['id' => 'b', 'tags' => []],
                ], ListColumnsMother::tagsSchema()),
            ),
        );
    }

    public function test_unpack_emits_one_row_per_element(): void
    {
        static::assertSame(
            [
                ['id' => 'a', 'tags' => ['x', 'y'], 'u.tag' => 'x'],
                ['id' => 'a', 'tags' => ['x', 'y'], 'u.tag' => 'y'],
            ],
            ExpandingProcessorContext::rows(
                flow_context(config()),
                (new ExpandingProcessor(
                    'u',
                    structure(['tag' => ref('tags')->expand()])->unpack(schema(str_schema('tag'))),
                    RequiredColumns::all(),
                ))->bind(ListColumnsMother::tagsSchema())->step,
                array_to_rows([['id' => 'a', 'tags' => ['x', 'y']]], ListColumnsMother::tagsSchema()),
            ),
        );
    }

    public function test_bind_rebinds_against_another_input(): void
    {
        static::assertEquals(
            schema(
                list_schema('tags', type_list(type_integer())),
                structure_schema('s', type_structure(['tag' => type_integer()])),
            ),
            (new ExpandingProcessor('s', structure(['tag' => ref('tags')->expand()]), RequiredColumns::all()))
                ->bind(ListColumnsMother::tagsSchema())
                ->step->bind(schema(list_schema('tags', type_list(type_integer()))))
                ->output,
        );
    }

    public function test_a_definition_entry_is_declared_as_given(): void
    {
        $bound = (new ExpandingProcessor(
            str_schema('s', nullable: true),
            concat(ref('id'), ref('tags')->expand()),
            RequiredColumns::all(),
        ))->bind(ListColumnsMother::tagsSchema());

        static::assertEquals(
            schema(str_schema('id'), list_schema('tags', type_list(type_string())), str_schema('s', nullable: true)),
            $bound->output,
        );
        static::assertSame(
            [
                ['id' => 'a', 'tags' => ['x', 'y'], 's' => 'ax'],
                ['id' => 'a', 'tags' => ['x', 'y'], 's' => 'ay'],
            ],
            ExpandingProcessorContext::rows(flow_context(config()), $bound->step, array_to_rows([[
                'id' => 'a',
                'tags' => ['x', 'y'],
            ]], ListColumnsMother::tagsSchema())),
        );
    }

    public function test_a_batch_under_another_schema_than_the_bound_one_is_conformed(): void
    {
        $bound = (new ExpandingProcessor(
            's',
            structure(['tag' => ref('tags')->expand()]),
            RequiredColumns::all(),
        ))->bind(schema(
            str_schema('id'),
            list_schema('tags', type_list(type_string())),
            str_schema('note', nullable: true),
        ));
        $batch = ExpandingProcessorContext::batches(flow_context(config()), $bound->step, array_to_rows([[
            'id' => 'a',
            'tags' => ['x'],
        ]], ListColumnsMother::tagsSchema()))[0];

        static::assertEquals(
            schema(
                str_schema('id'),
                list_schema('tags', type_list(type_string())),
                str_schema('note', nullable: true),
                structure_schema('s', type_structure(['tag' => type_string()])),
            ),
            $batch->schema(),
        );
        static::assertSame([['id' => 'a', 'tags' => ['x'], 'note' => null, 's' => ['tag' => 'x']]], $batch->toArray());
    }

    public function test_a_null_under_a_not_null_definition_names_its_output_row(): void
    {
        $input = schema(list_schema('tags', type_list(type_optional(type_string()))));

        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "s" (row 1)');

        ExpandingProcessorContext::rows(
            flow_context(config()),
            (new ExpandingProcessor(str_schema('s'), ref('tags')->expand()->coalesce(), RequiredColumns::all()))->bind(
                $input,
            )->step,
            array_to_rows([['tags' => ['x']], ['tags' => [null]]], $input),
        );
    }

    /**
     * @param array<string, mixed> $override
     * @param list<mixed> $values
     */
    #[DataProvider('zipped_expansions')]
    public function test_zipped_expands_give_one_value_per_zipped_position(
        ScalarFunction $tree,
        array $override,
        array $values,
    ): void {
        static::assertSame($values, array_column(
            ExpandingProcessorContext::rows(
                flow_context(config()),
                new ExpandingProcessor('v', $tree, RequiredColumns::only('v')),
                ListColumnsMother::rows($override),
            ),
            'v',
        ));
    }

    /**
     * @return Generator<string, array{ScalarFunction, array<string, mixed>, list<mixed>}>
     */
    public static function zipped_expansions(): Generator
    {
        yield 'one expand' => [
            structure(['tag' => ref('tags')->expand()]),
            [],
            [['tag' => 'x'], ['tag' => 'y']],
        ];
        yield 'zip pads the shorter' => [
            structure(['n' => ref('nums')->expand(), 'tag' => ref('tags')->expand()]),
            [],
            [['n' => 1, 'tag' => 'x'], ['n' => 2, 'tag' => 'y'], ['n' => 3, 'tag' => null]],
        ];
        yield 'concat skips a padded null' => [
            concat(ref('nums')->expand(), ref('tags')->expand()),
            [],
            ['1x', '2y', '3'],
        ];
        yield 'sole empty list' => [
            structure(['tag' => ref('tags')->expand()]),
            ['tags' => []],
            [],
        ];
        yield 'empty beside non-empty' => [
            structure(['n' => ref('nums')->expand(), 'tag' => ref('tags')->expand()]),
            ['nums' => [1, 2], 'tags' => []],
            [['n' => 1, 'tag' => null], ['n' => 2, 'tag' => null]],
        ];
        yield 'onEach operand' => [
            ref('lists')->expand()->onEach(concat(ref('element'), lit('!'))),
            [],
            [['q!', 'r!'], ['s!']],
        ];

        $tags = ref('tags')->expand();

        yield 'a node holding a reference, used twice, gives two padded columns' => [
            structure(['a' => $tags, 'b' => $tags]),
            [],
            [['a' => 'x', 'b' => 'x'], ['a' => 'y', 'b' => 'y']],
        ];

        $literal = lit(['x', 'y'])->expand();

        yield 'a node without references, used twice, gives one column' => [
            structure(['a' => $literal, 'b' => $literal]),
            [],
            [['a' => 'x', 'b' => 'x'], ['a' => 'y', 'b' => 'y']],
        ];
        yield 'branch not taken still expands' => [
            when(ref('id')->equals(lit('b')), ref('tags')->expand(), lit('none')),
            [],
            ['none', 'none'],
        ];
        yield 'a null list gives no values' => [
            structure(['tag' => ref('tags')->expand()]),
            ['tags' => null],
            [],
        ];
        yield 'a when guard does not stop the expand of a null list' => [
            when(ref('tags')->isNull(), lit('none'), ref('tags')->expand()),
            ['tags' => null],
            [],
        ];
    }

    public function test_a_padded_null_follows_the_null_rule_of_the_function_reading_it(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Plus function requires non-null values');

        ExpandingProcessorContext::rows(
            flow_context(config()),
            new ExpandingProcessor(
                'v',
                structure([
                    'n' => ref('nums')->expand()->plus(lit(1)),
                    't' => ref('tags')->expand(),
                ]),
                RequiredColumns::only('v'),
            ),
            ListColumnsMother::rows(['nums' => [1, 2], 'tags' => ['x', 'y', 'z']]),
        );
    }

    public function test_an_expand_over_a_map_reads_its_values_by_position(): void
    {
        static::assertSame(
            [['v' => ['v' => 1]], ['v' => ['v' => 2]]],
            ExpandingProcessorContext::rows(
                flow_context(config()),
                new ExpandingProcessor('v', structure(['v' => ref('m')->expand()]), RequiredColumns::only('v')),
                array_to_rows([['m' => [
                    'a' => 1,
                    'b' => 2,
                ]]], schema(map_schema('m', type_map(type_string(), type_integer())))),
            ),
        );
    }

    public function test_the_synthesized_column_does_not_shadow_an_input_column(): void
    {
        static::assertSame(
            [['v' => ['c' => 'kept', 't' => 'x']]],
            ExpandingProcessorContext::rows(
                flow_context(config()),
                new ExpandingProcessor(
                    'v',
                    structure(['c' => ref("\0expand:0"), 't' => ref('tags')->expand()]),
                    RequiredColumns::only('v'),
                ),
                array_to_rows(
                    [["\0expand:0" => 'kept', 'tags' => ['x']]],
                    schema(str_schema("\0expand:0"), list_schema('tags', type_list(type_string()))),
                ),
            ),
        );
    }

    #[DataProvider('expands_inside_an_expand')]
    public function test_bind_refuses_an_expand_inside_an_expand(ScalarFunction $function): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'array_expand() cannot contain another array_expand(). Expand one level per withEntry().',
        );

        (new ExpandingProcessor('v', $function, RequiredColumns::all()))->bind(ListColumnsMother::schema());
    }

    /**
     * @return Generator<string, array{ScalarFunction}>
     */
    public static function expands_inside_an_expand(): Generator
    {
        yield 'at the root' => [ref('lists')->expand()->expand()];
        yield 'inside a structure' => [structure(['v' => ref('lists')->expand()->expand()])];
    }

    public function test_an_unbound_expand_inside_an_expand_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'array_expand() cannot contain another array_expand(). Expand one level per withEntry().',
        );

        ExpandingProcessorContext::rows(
            flow_context(config()),
            new ExpandingProcessor('v', ref('lists')->expand()->expand(), RequiredColumns::all()),
            ListColumnsMother::rows(),
        );
    }
}
