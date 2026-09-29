<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Processor\MergeSortProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\RecordingBucketsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ExternalSortMother;
use Generator;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;

final class MergeSortProcessorTest extends FlowTestCase
{
    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(int_schema('id'));

        static::assertEquals(
            $input,
            (new MergeSortProcessor(ExternalSortMother::inMemory(refs('id'))))->bind($input)->output,
        );
    }

    public function test_process_sorts_through_the_external_sort(): void
    {
        $storage = new RecordingBucketsStorage(new MemoryBuckets());
        $input = (static function (): Generator {
            yield array_to_rows([['id' => 3], ['id' => 1]], schema(int_schema('id')));
            yield array_to_rows([['id' => 2]], schema(int_schema('id')));
        })();

        $result = iterator_to_array(
            (new MergeSortProcessor(ExternalSortMother::spilling(refs('id'), $storage)))->process(
                $input,
                flow_context(config()),
            ),
            false,
        );

        static::assertSame(
            [1, 2, 3],
            array_merge(...array_map(static fn(Rows $rows): array => $rows->reduceToArray('id'), $result)),
        );
        static::assertNotSame([], $storage->appended);
    }
}
