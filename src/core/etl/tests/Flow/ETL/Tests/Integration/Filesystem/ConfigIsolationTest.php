<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\to_callable;
use function Flow\Floe\DSL\floe_options;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;

final class ConfigIsolationTest extends FlowIntegrationTestCase
{
    public function test_a_failed_run_does_not_leave_a_stream_registered_on_a_shared_config(): void
    {
        $config = config();
        $destination = $this->cacheDir->suffix('crashed.floe');
        $rows = [];

        for ($id = 1; $id <= 200; $id++) {
            $rows[] = ['id' => $id, 'name' => 'padding-so-the-writer-flushes-before-the-crash-' . $id];
        }

        try {
            data_frame($config)
                ->read(from_array($rows))
                ->batchSize(10)
                ->write(to_floe($destination, options: floe_options(buffer_size: 64)))
                ->write(to_callable(static function (Rows $rows, FlowContext $context): void {
                    if ($rows->first()->valueOf('id') === 101) {
                        throw new RuntimeException('aborted mid run');
                    }
                }))
                ->run();
            static::fail('The first run was expected to fail');
        } catch (RuntimeException $e) {
            static::assertSame('aborted mid run', $e->getMessage());
        }

        // the dead run took its own file with it, so the destination is exactly as it was before the run
        static::assertNull($this->fs()->status($destination));

        data_frame($config)->read(from_array($rows))->write(to_floe($destination))->run();

        $retried = 0;

        foreach (data_frame($config)->read(from_floe($destination))->get() as $batch) {
            $retried += $batch->count();
        }

        static::assertSame(200, $retried);
    }

    public function test_a_dead_run_does_not_bleed_into_the_next_on_the_same_sink(): void
    {
        $config = config();
        $destination = $this->cacheDir->suffix('reused_sink.floe');

        // ONE sink reused across both runs - a per-sink registry would carry run 1's stream into run 2
        $sink = to_floe($destination);

        try {
            data_frame($config)
                ->read(from_array([['id' => 1]]))
                ->write($sink)
                ->write(to_callable(static function (Rows $rows, FlowContext $context): void {
                    throw new RuntimeException('aborted');
                }))
                ->run();
            static::fail('The first run was expected to fail');
        } catch (RuntimeException $e) {
            static::assertSame('aborted', $e->getMessage());
        }

        static::assertNull($this->fs()->status($destination));

        // the same sink again: run 1's stream must not be reachable, and its rows must not appear here
        data_frame($config)
            ->read(from_array([['id' => 2]]))
            ->write($sink)
            ->run();

        $ids = [];

        foreach (data_frame($config)->read(from_floe($destination))->get() as $batch) {
            foreach ($batch as $row) {
                $ids[] = $row->valueOf('id');
            }
        }

        static::assertSame([2], $ids);
    }

    public function test_save_mode_does_not_leak_from_one_data_frame_to_the_next(): void
    {
        $config = config();
        $destination = $this->cacheDir->suffix('second_frame.floe');

        data_frame($config)
            ->read(from_array([['id' => 1]]))
            ->write(to_floe($this->cacheDir->suffix('first_frame.floe'))->saveMode(overwrite()))
            ->run();

        $this->fs()->writeTo($destination)->append('pre-existing')->close();

        $this->expectExceptionMessage('already exists, please change path to different or set different SaveMode');

        data_frame($config)
            ->read(from_array([['id' => 2]]))
            ->write(to_floe($destination))
            ->run();
    }
}
