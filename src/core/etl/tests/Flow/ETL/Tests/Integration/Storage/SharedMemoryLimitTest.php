<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Storage;

use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Double\HoldingExtractor;
use Flow\ETL\Tests\Double\RecordingRowsStorage;
use Flow\ETL\Tests\Double\ReportedMemoryBackend;
use Flow\ETL\Tests\Double\SpyRowsStorage;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Values\Memory\MemoryLimit;
use Flow\ETL\Values\Memory\Unit;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\hash_join;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function getenv;
use function ini_get;
use function ini_set;
use function memory_get_usage;
use function putenv;

final class SharedMemoryLimitTest extends FlowIntegrationTestCase
{
    public function test_a_sort_after_a_join_build_gets_its_own_limit(): void
    {
        $megabyte = 1_000_000;
        $memoryLimit = (string) ini_get('memory_limit');
        ini_set('memory_limit', '-1');

        try {
            // another step already holds 800 MB; the join build adds 50 MB a batch, each sorted batch 100 MB
            $backend = new ReportedMemoryBackend(800 * $megabyte);
            $limit = Unit::fromBytes(1_000 * $megabyte);
            $joinStorage = new SpyRowsStorage(new MemoryRowsStorage());
            $sortStorage = new RecordingRowsStorage(new MemoryRowsStorage());
            $schema = schema(int_schema('id'));

            static::assertSame(
                [1, 2, 3],
                data_frame(
                    config_builder()
                        ->backend($backend)
                        ->join(hash_join()->storage($joinStorage)->memoryLimit($limit))
                        ->sort(external_sort()->storage($sortStorage)->memoryLimit($limit))
                        ->build(),
                )
                    ->read(new HoldingExtractor($backend, 100 * $megabyte, [
                        array_to_rows([['id' => 2]], $schema),
                        array_to_rows([['id' => 3]], $schema),
                        array_to_rows([['id' => 1]], $schema),
                    ]))
                    ->join(data_frame()->read(new HoldingExtractor($backend, 50 * $megabyte, [
                        array_to_rows([['id' => 1], ['id' => 2]], $schema),
                        array_to_rows([['id' => 3]], $schema),
                    ])), join_on(['id' => 'id'], 'r_'))
                    ->sortBy(ref('id'))
                    ->fetch()
                    ->reduceToArray('id'),
            );
            static::assertSame([], $joinStorage->appendedRows(), 'the join build fits and stays in memory');
            static::assertSame([], $sortStorage->appended, 'the memory held before the sort is not its own');
        } finally {
            ini_set('memory_limit', $memoryLimit);
        }
    }

    public function test_a_join_build_on_the_first_pull_is_not_the_sorts_own_memory(): void
    {
        $megabyte = 1_000_000;
        $memoryLimit = (string) ini_get('memory_limit');
        ini_set('memory_limit', '-1');

        try {
            // the join builds 400 MB when the sort first pulls; each sorted batch adds 100 MB
            $backend = new ReportedMemoryBackend(0);
            $limit = Unit::fromBytes(1_000 * $megabyte);
            $sortStorage = new RecordingRowsStorage(new MemoryRowsStorage());
            $schema = schema(int_schema('id'));

            static::assertSame(
                [1, 2, 3],
                data_frame(
                    config_builder()
                        ->backend($backend)
                        ->join(hash_join()->storage(new MemoryRowsStorage())->memoryLimit($limit))
                        ->sort(external_sort()->storage($sortStorage)->memoryLimit($limit))
                        ->build(),
                )
                    ->read(new HoldingExtractor($backend, 100 * $megabyte, [
                        array_to_rows([['id' => 2]], $schema),
                        array_to_rows([['id' => 3]], $schema),
                        array_to_rows([['id' => 1]], $schema),
                    ]))
                    ->join(data_frame()->read(new HoldingExtractor($backend, 200 * $megabyte, [
                        array_to_rows([['id' => 1], ['id' => 2]], $schema),
                        array_to_rows([['id' => 3]], $schema),
                    ])), join_on(['id' => 'id'], 'r_'))
                    ->sortBy(ref('id'))
                    ->fetch()
                    ->reduceToArray('id'),
            );
            static::assertSame([], $sortStorage->appended, 'the join build happened before the sort saw a row');
        } finally {
            ini_set('memory_limit', $memoryLimit);
        }
    }

    public function test_a_sort_spills_at_the_floor_when_the_process_is_over_its_cap(): void
    {
        $megabyte = 1_000_000;
        $memoryLimit = (string) ini_get('memory_limit');
        $environment = getenv(MemoryLimit::ENV);
        static::assertNotFalse(
            ini_set('memory_limit', (string) (memory_get_usage(true) + Unit::fromMb(512)->inBytes())),
            'PHP refuses a memory_limit under the heap it already holds',
        );
        putenv(MemoryLimit::ENV);

        try {
            $backend = new ReportedMemoryBackend(800 * $megabyte);
            $limit = Unit::fromBytes(1_000 * $megabyte);
            $sortStorage = new RecordingRowsStorage(new MemoryRowsStorage());
            $schema = schema(int_schema('id'));

            static::assertSame(
                [1, 2, 3],
                data_frame(
                    config_builder()
                        ->backend($backend)
                        ->join(hash_join()->storage(new MemoryRowsStorage())->memoryLimit($limit))
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
                    ->fetch()
                    ->reduceToArray('id'),
            );
            static::assertNotSame(
                [],
                $sortStorage->appended,
                'the process is over its cap: the sort spills at the floor',
            );
        } finally {
            ini_set('memory_limit', $memoryLimit);
            putenv($environment === false ? MemoryLimit::ENV : MemoryLimit::ENV . '=' . $environment);
        }
    }
}
