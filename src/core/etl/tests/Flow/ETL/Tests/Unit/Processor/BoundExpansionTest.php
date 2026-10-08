<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Plan\RequiredColumns;
use Flow\ETL\Processor\BoundExpansion;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ListColumnsMother;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class BoundExpansionTest extends FlowTestCase
{
    public function test_an_expand_written_in_place_never_gathers_the_column_it_overwrites(): void
    {
        $bound = BoundExpansion::of(
            'tags',
            ref('tags')->expand(),
            RequiredColumns::all(),
            schema(str_schema('id'), list_schema('tags', type_list(type_string())), str_schema('note')),
        );

        static::assertSame(['id', 'note'], $bound->gathered);
        static::assertEquals(schema(str_schema('id'), str_schema('tags'), str_schema('note')), $bound->output);
    }

    public function test_a_new_column_selected_alone_gathers_no_input_column(): void
    {
        $bound = BoundExpansion::of(
            'record',
            ref('tags')->expand(),
            RequiredColumns::only('record'),
            ListColumnsMother::tagsSchema(),
        );

        static::assertSame([], $bound->gathered);
        static::assertEquals(schema(str_schema('record')), $bound->output);
    }

    public function test_all_but_a_column_gathers_every_other_input_column(): void
    {
        $bound = BoundExpansion::of(
            'tag',
            ref('tags')->expand(),
            RequiredColumns::allBut('tags'),
            ListColumnsMother::schema(),
        );

        static::assertSame(['id', 'nums', 'lists', 'flags'], $bound->gathered);
        static::assertSame(['id', 'nums', 'lists', 'flags', 'tag'], $bound->output->references()->names());
    }

    public function test_a_column_the_root_reads_is_gathered_but_not_output(): void
    {
        $bound = BoundExpansion::of(
            's',
            concat(ref('tags')->expand(), ref('id')),
            RequiredColumns::only('s'),
            ListColumnsMother::tagsSchema(),
        );

        static::assertSame(['id'], $bound->gathered);
        static::assertEquals(schema(str_schema('s')), $bound->output);
    }

    public function test_an_unpack_gathers_neither_the_columns_it_writes_nor_drops_a_same_prefixed_one(): void
    {
        $bound = BoundExpansion::of(
            's',
            structure(['a' => ref('tags')->expand()])->unpack(schema(str_schema('a'))),
            RequiredColumns::all(),
            schema(list_schema('tags', type_list(type_string())), int_schema('s.a'), str_schema('s.other')),
        );

        static::assertSame(['tags', 's.other'], $bound->gathered);
        static::assertSame(
            [['tags' => ['x'], 's.a' => 'x', 's.other' => 'kept']],
            iterator_to_array(
                $bound->chunks(
                    array_to_rows(
                        [['tags' => ['x'], 's.a' => 1, 's.other' => 'kept']],
                        schema(list_schema('tags', type_list(type_string())), int_schema('s.a'), str_schema('s.other')),
                    ),
                    flow_context(),
                    1000,
                ),
                false,
            )[0]->toArray(),
        );
    }

    public function test_chunks_split_the_output_into_batches_of_at_most_the_batch_size(): void
    {
        $bound = BoundExpansion::of(
            'tag',
            ref('tags')->expand(),
            RequiredColumns::only('tag'),
            ListColumnsMother::tagsSchema(),
        );

        static::assertSame(
            [[['tag' => 'x'], ['tag' => 'y']], [['tag' => 'z']]],
            array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array(
                    $bound->chunks(
                        array_to_rows([['id' => 'a', 'tags' => ['x', 'y', 'z']]], ListColumnsMother::tagsSchema()),
                        flow_context(),
                        2,
                    ),
                    false,
                ),
            ),
        );
    }

    public function test_an_input_batch_that_expands_to_nothing_yields_one_empty_batch(): void
    {
        $bound = BoundExpansion::of(
            'tag',
            ref('tags')->expand(),
            RequiredColumns::all(),
            ListColumnsMother::tagsSchema(),
        );
        $batches = iterator_to_array(
            $bound->chunks(
                array_to_rows([['id' => 'a', 'tags' => []]], ListColumnsMother::tagsSchema()),
                flow_context(),
                2,
            ),
            false,
        );

        static::assertCount(1, $batches);
        static::assertSame(0, $batches[0]->count());
        static::assertEquals($bound->output, $batches[0]->schema());
    }

    public function test_a_function_without_an_expand_is_not_an_expansion(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('holds no array_expand(), it is not an expansion');

        BoundExpansion::of(
            's',
            structure(['id' => ref('id')]),
            RequiredColumns::all(),
            ListColumnsMother::tagsSchema(),
        );
    }

    public function test_the_output_keeps_the_column_order_of_a_full_expansion(): void
    {
        static::assertSame(
            ['id', 's', 'note'],
            BoundExpansion::of(
                's',
                structure(['t' => ref('s')->expand()]),
                RequiredColumns::allBut('x'),
                schema(str_schema('id'), list_schema('s', type_list(type_string())), str_schema('note')),
            )->output->references()->names(),
        );
        static::assertEquals(
            type_structure(['t' => type_string()]),
            BoundExpansion::of(
                's',
                structure(['t' => ref('s')->expand()]),
                RequiredColumns::all(),
                schema(str_schema('id'), list_schema('s', type_list(type_string())), str_schema('note')),
            )->output->get('s')->type(),
        );
    }
}
