<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Rows;

use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows\RecordBatch;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class RecordBatchTest extends FlowTestCase
{
    public function test_a_batch_is_handed_out_once_it_is_full_and_the_rest_on_flush(): void
    {
        $batch = new RecordBatch(schema(int_schema('id')), new PhpBackend(), 2);

        static::assertNull($batch->add(['id' => 1]));
        static::assertSame([['id' => 1], ['id' => 2]], $batch->add(['id' => 2])?->toArray());
        static::assertNull($batch->add(['id' => 3]));
        static::assertSame([['id' => 3]], $batch->flush()?->toArray());
        static::assertNull($batch->flush());
    }

    public function test_nothing_added_flushes_nothing(): void
    {
        static::assertNull((new RecordBatch(schema(int_schema('id')), new PhpBackend(), 1))->flush());
    }

    public function test_a_refused_record_is_reported_at_its_position_among_every_record_added(): void
    {
        $batch = new RecordBatch(schema(int_schema('id')), new PhpBackend(), 2);
        $batch->add(['id' => 1]);
        $batch->add(['id' => 2]);
        $batch->add(['id' => 3]);

        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "id" (row 3)');

        $batch->add(['id' => 'x']);
    }
}
