<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Dataset;

use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Tests\Double\HoldingExtractor;
use Flow\ETL\Tests\Double\RecordingBucketsStorage;
use Flow\ETL\Tests\Double\ReportedMemoryBackend;
use Flow\ETL\Tests\Double\SpyBucketsStorage;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\hash_join;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function memory_get_usage;

final class SharedMemoryLimitTest extends FlowIntegrationTestCase
{
    public function test_a_sort_after_a_join_build_spills_once_both_together_pass_the_limit(): void
    {
        $megabyte = 1_000_000;

        // another step already holds 800 MB; the join build adds 50 MB a batch (fits), each sorted batch 200 MB
        $backend = new ReportedMemoryBackend(800 * $megabyte);
        $limit = Unit::fromBytes(memory_get_usage(false) + (1_000 * $megabyte));
        $joinStorage = new SpyBucketsStorage(new MemoryBuckets());
        $sortStorage = new RecordingBucketsStorage(new MemoryBuckets());
        $schema = schema(int_schema('id'));

        $sorted = data_frame(
            config_builder()
                ->backend($backend)
                ->join(hash_join()->storage($joinStorage)->memoryLimit($limit))
                ->sort(external_sort()->storage($sortStorage)->memoryLimit($limit))
                ->build(),
        )
            ->read(new HoldingExtractor($backend, 200 * $megabyte, [
                array_to_rows([['id' => 2]], $schema),
                array_to_rows([['id' => 3]], $schema),
                array_to_rows([['id' => 1]], $schema),
            ]))
            ->join(data_frame()->read(new HoldingExtractor($backend, 50 * $megabyte, [
                array_to_rows([['id' => 1], ['id' => 2]], $schema),
                array_to_rows([['id' => 3]], $schema),
            ])), join_on(['id' => 'id'], 'r_'))
            ->sortBy(ref('id'))
            ->fetch();

        static::assertSame([1, 2, 3], $sorted->reduceToArray('id'));
        static::assertSame([], $joinStorage->appendedRows(), 'the join build fits and stays in memory');
        static::assertNotSame([], $sortStorage->appended, 'the sort must spill: the process passed the limit');
    }
}
