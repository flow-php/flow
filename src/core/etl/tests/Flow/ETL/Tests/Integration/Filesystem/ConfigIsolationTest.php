<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\to_callable;
use function Flow\Floe\DSL\floe_options;
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

        $partialContent = $this->fs()->readFrom($destination)->content();
        static::assertNotSame('', $partialContent);

        try {
            data_frame($config)->read(from_array($rows))->write(to_floe($destination))->run();
            static::fail('The retry was expected to refuse writing to the existing destination');
        } catch (RuntimeException $e) {
            static::assertStringContainsString('already exists', $e->getMessage());
        }

        static::assertSame($partialContent, $this->fs()->readFrom($destination)->content());
    }

    public function test_aborted_runs_do_not_accumulate_stream_registrations(): void
    {
        $config = config();
        $aborted = 0;

        for ($run = 1; $run <= 3; $run++) {
            try {
                data_frame($config)
                    ->read(from_array([['id' => 1]]))
                    ->write(to_floe($this->cacheDir->suffix("aborted_{$run}.floe")))
                    ->write(to_callable(static function (Rows $rows, FlowContext $context): void {
                        throw new RuntimeException('aborted');
                    }))
                    ->run();
            } catch (RuntimeException) {
                $aborted++;
            }
        }

        static::assertSame(3, $aborted);
        static::assertCount(0, flow_context($config)->streams());
    }

    public function test_save_mode_does_not_leak_from_one_data_frame_to_the_next(): void
    {
        $config = config();
        $destination = $this->cacheDir->suffix('second_frame.floe');

        data_frame($config)
            ->read(from_array([['id' => 1]]))
            ->saveMode(overwrite())
            ->write(to_floe($this->cacheDir->suffix('first_frame.floe')))
            ->run();

        $this->fs()->writeTo($destination)->append('pre-existing')->close();

        $this->expectExceptionMessage('already exists, please change path to different or set different SaveMode');

        data_frame($config)
            ->read(from_array([['id' => 2]]))
            ->write(to_floe($destination))
            ->run();
    }
}
