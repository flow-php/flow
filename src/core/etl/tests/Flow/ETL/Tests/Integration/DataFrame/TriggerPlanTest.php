<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\API\Transactional;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\SpyTransformer;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;

final class TriggerPlanTest extends FlowIntegrationTestCase
{
    public function test_a_transform_after_the_last_write_sees_every_row_under_run(): void
    {
        $loader = new SpyLoader();
        $afterWrite = new SpyTransformer();

        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->batchSize(1)
            ->write($loader)
            ->with($afterWrite)
            ->run();

        static::assertSame(3, $afterWrite->seen);
        static::assertSame([1, 1, 1], $loader->loadedRowCounts());
    }

    public function test_run_analyze_counts_the_rows_of_a_frame_with_a_write(): void
    {
        $report = df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->write(new SpyLoader())
            ->run(analyze: analyze()->withSchema());

        static::assertSame(3, $report->statistics()->totalRows());
    }

    public function test_run_analyze_counts_the_rows_of_a_frame_without_a_write(): void
    {
        $report = df()->read(from_array([['id' => 1], ['id' => 2]]))->run(analyze: analyze()->withSchema());

        static::assertSame(2, $report->statistics()->totalRows());
    }

    public function test_one_write_is_written_exactly_once(): void
    {
        $loader = new SpyLoader();

        df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->batchSize(1)
            ->write($loader)
            ->run();

        static::assertSame([1, 1], $loader->loadedRowCounts());
    }

    public function test_two_writes_are_each_written_exactly_once_in_write_order(): void
    {
        $first = new SpyLoader();
        $second = new SpyLoader();

        df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->batchSize(1)
            ->write($first)
            ->write($second)
            ->run();

        static::assertSame([1, 1], $first->loadedRowCounts());
        static::assertSame([1, 1], $second->loadedRowCounts());
    }

    public function test_a_transaction_is_written_exactly_once(): void
    {
        $loader = new SpyLoader();
        $transaction = new RecordingTransaction();

        df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->batchSize(1)
            ->write(new Transactional($transaction, $loader))
            ->run();

        static::assertSame([1, 1], $loader->loadedRowCounts());
    }
}
