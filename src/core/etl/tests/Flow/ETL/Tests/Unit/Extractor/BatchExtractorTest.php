<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\Double\VaryingBatchesExtractor;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\batches;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class BatchExtractorTest extends FlowTestCase
{
    public function test_a_buffer_spanning_child_batches_answers_to_the_first_batch_schema(): void
    {
        // the second child batch omits a column the first declares nullable - the buffer that spans
        // both must carry one schema, and the later rows are matched to it rather than mislabelled
        $child = new VaryingBatchesExtractor(
            rows(schema(int_schema('id'), str_schema('name', nullable: true)), row(['id' => 1, 'name' => 'a'])),
            rows(schema(int_schema('id')), row(['id' => 2])),
        );

        $batches = iterator_to_array(batches($child, 2)->extract(flow_context()), false);

        static::assertCount(1, $batches);
        static::assertSame(['id', 'name'], $batches[0]->schema()->references()->names());
        static::assertSame([['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null]], $batches[0]->toArray());
    }

    public function test_a_later_child_batch_that_widens_the_shape_is_refused(): void
    {
        $child = new VaryingBatchesExtractor(
            rows(schema(int_schema('id')), row(['id' => 1])),
            rows(schema(int_schema('id'), str_schema('extra')), row(['id' => 2, 'extra' => 'x'])),
        );

        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "extra" (row 0) is not declared by the schema');

        iterator_to_array(batches($child, 2)->extract(flow_context()), false);
    }

    public function test_chunk_extractor(): void
    {
        $extractor = batches(new FakeExtractor($batches = 100), $chunkSize = 10);

        self::assertExtractedBatchesCount($batches / $chunkSize, $extractor);
    }

    public function test_chunk_extractor_with_chunk_size_greater_than_(): void
    {
        $extractor = batches(new FakeExtractor(total: 20), size: 25);

        self::assertExtractedBatchesCount(1, $extractor);
    }

    public function test_with_schema_does_not_leak_into_a_second_pipeline(): void
    {
        $child = from_rows(rows(schema(int_schema('id')), row(['id' => 1])));

        iterator_to_array(
            batches($child, 1)
                ->withSchema(schema(int_schema('id'), str_schema('name', nullable: true)))
                ->extract(flow_context()),
            false,
        );

        static::assertTrue($child->schema()->isSame(schema(int_schema('id'))));
        static::assertSame(
            [['id' => 1]],
            iterator_to_array(batches($child, 1)->extract(flow_context()), false)[0]->toArray(),
        );
    }
}
