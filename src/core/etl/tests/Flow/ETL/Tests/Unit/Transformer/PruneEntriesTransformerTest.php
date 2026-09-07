<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\PruneEntriesTransformer;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class PruneEntriesTransformerTest extends FlowTestCase
{
    public function test_bind_keeps_only_the_declared_refs_in_ref_order(): void
    {
        static::assertEquals(
            schema(str_schema('name'), int_schema('id')),
            (new PruneEntriesTransformer(ref('name'), ref('id'), ref('missing')))->bind(schema(
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
            (new PruneEntriesTransformer(ref('missing')))->bind(schema(int_schema('id')))->output,
        );
    }

    public function test_keeping_only_the_given_entries(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'a']],
            (new PruneEntriesTransformer(ref('id'), ref('name')))
                ->transform(
                    rows(
                        schema(int_schema('id'), str_schema('name'), int_schema('dropped')),
                        row(['id' => 1, 'name' => 'a', 'dropped' => 9]),
                    ),
                    flow_context(),
                )
                ->toArray(),
        );
    }

    public function test_entry_missing_from_a_row_is_padded_from_its_nullable_declaration(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null]],
            (new PruneEntriesTransformer(ref('id'), ref('name')))
                ->transform(
                    rows(
                        schema(int_schema('id'), str_schema('name', nullable: true)),
                        row(['id' => 1, 'name' => 'a']),
                        row(['id' => 2]),
                    ),
                    flow_context(),
                )
                ->toArray(),
        );
    }

    public function test_row_without_any_of_the_given_entries_becomes_empty(): void
    {
        static::assertSame(
            [[]],
            (new PruneEntriesTransformer(ref('missing')))
                ->transform(rows(schema(int_schema('id')), row(['id' => 1])), flow_context())
                ->toArray(),
        );
    }
}
