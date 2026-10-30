<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\Bucketing;

use Flow\ETL\Kernels\Bucketing\BucketRun;
use Flow\ETL\Kernels\Bucketing\Buckets;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Context\RowsStorageContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\BucketMother;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class BucketRunTest extends FlowTestCase
{
    public function test_remove_removes_through_its_own_buckets(): void
    {
        $a = new Buckets($storageA = new MemoryRowsStorage());
        $b = new Buckets($storageB = new MemoryRowsStorage());

        $storageA->append('shared-id', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $a->add(BucketMother::withTotalRows('shared-id', 1));
        $storageB->append('shared-id', array_to_rows([['id' => 2]], schema(int_schema('id'))));
        $b->add(BucketMother::withTotalRows('shared-id', 1));

        (new BucketRun('shared-id', $a))->remove();

        static::assertSame([], RowsStorageContext::rows($storageA->get('shared-id')));
        static::assertCount(1, RowsStorageContext::rows($storageB->get('shared-id')));
    }

    public function test_rows_are_read_through_the_buckets_that_wrote_them(): void
    {
        $a = new Buckets($storageA = new MemoryRowsStorage());
        $b = new Buckets($storageB = new MemoryRowsStorage());

        $storageA->append('shared-id', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $storageB->append('shared-id', array_to_rows([['id' => 2]], schema(int_schema('id'))));

        static::assertSame([1], array_column(RowsStorageContext::rows((new BucketRun('shared-id', $a))->rows()), 'id'));
        static::assertSame([2], array_column(RowsStorageContext::rows((new BucketRun('shared-id', $b))->rows()), 'id'));
    }
}
