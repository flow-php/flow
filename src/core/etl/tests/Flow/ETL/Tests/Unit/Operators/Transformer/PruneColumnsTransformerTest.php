<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Transformer;

use Flow\ETL\Operators\Transformer\PruneColumnsTransformer;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\StepRoundTrip;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class PruneColumnsTransformerTest extends FlowTestCase
{
    public function test_bind_keeps_only_the_declared_refs_in_ref_order(): void
    {
        static::assertEquals(
            schema(str_schema('name'), int_schema('id')),
            (new PruneColumnsTransformer(ref('name'), ref('id'), ref('missing')))->bind(schema(
                int_schema('id'),
                str_schema('name'),
                int_schema('dropped'),
            ))->output,
        );
    }

    public function test_bind_with_no_ref_present_in_the_input_derives_an_empty_schema(): void
    {
        static::assertEquals(
            schema(),
            (new PruneColumnsTransformer(ref('missing')))->bind(schema(int_schema('id')))->output,
        );
    }

    public function test_keeping_only_the_given_entries(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'a']],
            (new PruneColumnsTransformer(ref('id'), ref('name')))
                ->transform(
                    array_to_rows(
                        [['id' => 1, 'name' => 'a', 'dropped' => 9]],
                        schema(int_schema('id'), str_schema('name'), int_schema('dropped')),
                    ),
                    flow_context()->backend(),
                )
                ->toArray(),
        );
    }

    public function test_entry_missing_from_a_row_is_padded_from_its_nullable_declaration(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null]],
            (new PruneColumnsTransformer(ref('id'), ref('name')))
                ->transform(
                    array_to_rows(
                        [['id' => 1, 'name' => 'a'], ['id' => 2]],
                        schema(int_schema('id'), str_schema('name', nullable: true)),
                    ),
                    flow_context()->backend(),
                )
                ->toArray(),
        );
    }

    public function test_row_without_any_of_the_given_entries_becomes_empty(): void
    {
        static::assertSame(
            [[]],
            (new PruneColumnsTransformer(ref('missing')))
                ->transform(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context()->backend())
                ->toArray(),
        );
    }

    public function test_describe_round_trips(): void
    {
        $transformer = new PruneColumnsTransformer('id');
        $rebuilt = DescribeRoundTrip::rebuilt($transformer);
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b'], ['id' => 3, 'name' => 'c']],
            schema(int_schema('id'), str_schema('name')),
        );
        $schema = schema(int_schema('id'), str_schema('name'));

        static::assertSame(
            StepRoundTrip::transformed($transformer, $schema, $rows, flow_context()),
            StepRoundTrip::transformed($rebuilt, $schema, $rows, flow_context()),
        );
    }
}
