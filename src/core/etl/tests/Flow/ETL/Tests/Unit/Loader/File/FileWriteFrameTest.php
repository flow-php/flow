<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader\File;

use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Loader\File\FileWriteFrame;
use Flow\ETL\Loader\File\PartitionRouter;
use Flow\ETL\Loader\Partitioning;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\SpyFileSinks;
use Flow\ETL\Tests\FlowTestCase;
use RuntimeException;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\telemetry_options;
use function Flow\ETL\DSL\to_array;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class FileWriteFrameTest extends FlowTestCase
{
    public function test_one_sink_per_partition_uri(): void
    {
        $filesystem = memory_filesystem();
        $sinks = new SpyFileSinks();
        $frame = new FileWriteFrame(
            $filesystem,
            path('memory://out/file.txt'),
            SaveMode::ExceptionIfExists,
            new PartitionRouter(partition_by('group')),
            $sinks,
        );
        $schema = schema(int_schema('id'), str_schema('group'));
        $output = [];

        $frame->write(
            array_to_rows([['id' => 1, 'group' => 'a'], ['id' => 2, 'group' => 'b']], $schema),
            flow_context(),
            to_array($output),
        );
        $frame->write(array_to_rows([['id' => 3, 'group' => 'a']], $schema), flow_context(), to_array($output));
        $frame->closure();

        static::assertSame(['memory://out/group=a/file.txt', 'memory://out/group=b/file.txt'], $sinks->opened);
        static::assertSame(
            'rows:1;rows:1;closed;',
            $filesystem->readFrom(path('memory://out/group=a/file.txt'))->content(),
        );
        static::assertSame('rows:1;closed;', $filesystem->readFrom(path('memory://out/group=b/file.txt'))->content());
    }

    public function test_closure_closes_every_sink_before_it_publishes(): void
    {
        $filesystem = memory_filesystem();
        $frame = new FileWriteFrame(
            $filesystem,
            path('memory://out/file.txt'),
            SaveMode::ExceptionIfExists,
            new PartitionRouter(Partitioning::none()),
            new SpyFileSinks(),
        );
        $output = [];

        $frame->write(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context(), to_array($output));
        $frame->closure();

        static::assertSame('rows:1;closed;', $filesystem->readFrom(path('memory://out/file.txt'))->content());
    }

    public function test_discard_closes_every_sink_and_leaves_no_file(): void
    {
        $filesystem = memory_filesystem();
        $sinks = new SpyFileSinks();
        $frame = new FileWriteFrame(
            $filesystem,
            path('memory://out/file.txt'),
            SaveMode::ExceptionIfExists,
            new PartitionRouter(Partitioning::none()),
            $sinks,
        );
        $output = [];

        $frame->write(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context(), to_array($output));
        $frame->discard();

        static::assertSame(['memory://out/file.txt'], $sinks->closed);
        static::assertNull($filesystem->status(path('memory://out/file.txt')));
    }

    public function test_a_throwing_sink_reports_the_load_as_failed(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_loading: true));
        $telemetry->flowContext->telemetry()->dataFrameStarted($telemetry->flowContext);
        $frame = new FileWriteFrame(
            memory_filesystem(),
            path('memory://out/file.txt'),
            SaveMode::ExceptionIfExists,
            new PartitionRouter(Partitioning::none()),
            new SpyFileSinks(failing: true),
        );
        $output = [];

        try {
            $frame->write(
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
                $telemetry->flowContext,
                to_array($output),
            );
            static::fail('the sink failure must reach the caller');
        } catch (RuntimeException $e) {
            static::assertSame('sink refused the batch', $e->getMessage());
        }

        $failed = $telemetry->spans->endedSpans();
        static::assertCount(1, $failed);
        static::assertTrue($failed[0]->status()?->isError() ?? false);
    }

    public function test_closure_closes_every_sink_then_rethrows_the_first_close_failure(): void
    {
        $sinks = new SpyFileSinks(failingClose: true);
        $frame = new FileWriteFrame(
            memory_filesystem(),
            path('memory://out/file.txt'),
            SaveMode::ExceptionIfExists,
            new PartitionRouter(partition_by('group')),
            $sinks,
        );
        $output = [];

        $frame->write(
            array_to_rows(
                [['id' => 1, 'group' => 'a'], ['id' => 2, 'group' => 'b']],
                schema(int_schema('id'), str_schema('group')),
            ),
            flow_context(),
            to_array($output),
        );

        try {
            $frame->closure();
            static::fail('a failing close must reach the caller');
        } catch (RuntimeException $e) {
            static::assertSame('close of memory://out/group=a/file.txt failed', $e->getMessage());
        }

        static::assertSame(['memory://out/group=a/file.txt', 'memory://out/group=b/file.txt'], $sinks->closed);
    }
}
