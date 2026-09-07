<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Extractor\CollectingExtractor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class CollectingExtractorTest extends FlowTestCase
{
    public function test_collecting_every_batch_into_one(): void
    {
        /** @var list<Rows> $collected */
        $collected = iterator_to_array(
            (new CollectingExtractor(from_rows(
                rows(schema(int_schema('id')), row(['id' => 1])),
                rows(schema(int_schema('id')), row(['id' => 2])),
            )))->extract(flow_context()),
            false,
        );

        static::assertCount(1, $collected);
        static::assertSame([['id' => 1], ['id' => 2]], $collected[0]->toArray());
    }

    public function test_with_schema_does_not_leak_into_a_second_pipeline(): void
    {
        $child = from_rows(rows(schema(int_schema('id')), row(['id' => 1])));

        iterator_to_array(
            (new CollectingExtractor($child))
                ->withSchema(schema(int_schema('id'), str_schema('name', nullable: true)))
                ->extract(flow_context()),
            false,
        );

        /** @var list<Rows> $rerun */
        $rerun = iterator_to_array((new CollectingExtractor($child))->extract(flow_context()), false);

        static::assertTrue($child->schema()->isSame(schema(int_schema('id'))));
        static::assertSame([['id' => 1]], $rerun[0]->toArray());
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(
            (new CollectingExtractor(from_rows(rows(schema(int_schema('id')), row(['id' => 1])))))->isRepeatable(),
        );
    }
}
