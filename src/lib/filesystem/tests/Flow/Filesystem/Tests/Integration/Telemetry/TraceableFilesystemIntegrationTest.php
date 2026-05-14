<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\Telemetry;

use Flow\Filesystem\Telemetry\FilesystemTelemetryAttributes;
use Flow\Filesystem\Tests\Mother\FilesystemTelemetryConfigMother;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\filesystem_telemetry_options;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Telemetry\DSL\memory_metric_processor;
use function Flow\Telemetry\DSL\memory_span_processor;
use function Flow\Telemetry\DSL\void_exporter;

final class TraceableFilesystemIntegrationTest extends TestCase
{
    protected function tearDown(): void
    {
        $testDir = path(__DIR__ . '/var');
        $localFs = native_local_filesystem();

        if ($localFs->status($testDir) !== null) {
            $localFs->rm($testDir);
        }
    }

    public function test_complete_read_write_workflow_produces_lifecycle_spans(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $fs = FilesystemTelemetryConfigMother::createTraceableFilesystem($spanProcessor);

        $testFile = path(__DIR__ . '/var/test_file.txt');
        $content = 'Hello, World!';

        $writeStream = $fs->writeTo($testFile);
        $writeStream->append($content);
        $writeStream->close();

        $readStream = $fs->readFrom($testFile);
        $readContent = $readStream->content();
        $readStream->close();

        static::assertSame($content, $readContent);

        $spans = $spanProcessor->endedSpans();
        $spanNames = \array_map(static fn($span) => $span->name(), $spans);

        static::assertContains('Write test_file.txt', $spanNames);
        static::assertContains('Read test_file.txt', $spanNames);
        static::assertCount(2, $spans);

        foreach ($spans as $span) {
            $status = $span->status();
            static::assertNotNull($status);
            static::assertTrue($status->isOk());
        }

        $destinationSpans = \array_values(\array_filter(
            $spans,
            static fn($span) => $span->name() === 'Write test_file.txt',
        ));
        static::assertCount(1, $destinationSpans);
        static::assertSame(
            \strlen($content),
            $destinationSpans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_WRITTEN],
        );

        $sourceSpans = \array_values(\array_filter($spans, static fn($span) => $span->name() === 'Read test_file.txt'));
        static::assertCount(1, $sourceSpans);
        static::assertSame(
            \strlen($content),
            $sourceSpans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_READ],
        );
    }

    public function test_filesystem_operations_do_not_create_spans(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $fs = FilesystemTelemetryConfigMother::createTraceableFilesystem($spanProcessor);

        $testFile = path(__DIR__ . '/var/no_fs_trace.txt');

        $fs->writeTo($testFile)->append('content')->close();
        $spanProcessor->reset();

        $fs->status($testFile);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(0, $spans);
    }

    public function test_from_resource_tracks_bytes_in_lifecycle_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $fs = FilesystemTelemetryConfigMother::createTraceableFilesystem($spanProcessor);

        $sourceFilePath = __DIR__ . '/../Fixtures/orders.csv';

        if (native_local_filesystem()->status(path($sourceFilePath)) === null) {
            static::markTestSkipped('Test fixture file not found');
        }

        $testFile = path(__DIR__ . '/var/from_resource_test.txt');
        $resource = \fopen($sourceFilePath, 'rb');
        static::assertIsResource($resource);

        $writeStream = $fs->writeTo($testFile);
        $writeStream->fromResource($resource);
        $writeStream->close();

        \fclose($resource);

        $spans = $spanProcessor->endedSpans();
        $destinationSpans = \array_values(\array_filter(
            $spans,
            static fn($span) => $span->name() === 'Write from_resource_test.txt',
        ));

        static::assertCount(1, $destinationSpans);
        $status = $destinationSpans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isOk());
        static::assertSame(
            'destination',
            $destinationSpans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_STREAM_TYPE],
        );
    }

    public function test_iterate_tracks_total_bytes_in_lifecycle_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $fs = FilesystemTelemetryConfigMother::createTraceableFilesystem($spanProcessor);

        $testFile = path(__DIR__ . '/var/iterate_test.txt');
        $content = "Line 1\nLine 2\nLine 3";

        $fs->writeTo($testFile)->append($content)->close();
        $spanProcessor->reset();

        $readStream = $fs->readFrom($testFile);
        $chunks = [];

        foreach ($readStream->iterate(5) as $chunk) {
            $chunks[] = $chunk;
        }
        $readStream->close();

        static::assertSame($content, \implode('', $chunks));

        $spans = $spanProcessor->endedSpans();
        $sourceSpans = \array_values(\array_filter(
            $spans,
            static fn($span) => $span->name() === 'Read iterate_test.txt',
        ));

        static::assertCount(1, $sourceSpans);
        static::assertSame(
            \strlen($content),
            $sourceSpans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_READ],
        );
    }

    public function test_list_operation_does_not_create_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $fs = FilesystemTelemetryConfigMother::createTraceableFilesystem($spanProcessor);

        $testDir = __DIR__ . '/var';

        $fs
            ->writeTo(path($testDir . '/file1.txt'))
            ->append('content1')
            ->close();
        $fs
            ->writeTo(path($testDir . '/file2.txt'))
            ->append('content2')
            ->close();
        $spanProcessor->reset();

        $files = \iterator_to_array($fs->list(path($testDir . '/*.txt')));

        static::assertCount(2, $files);

        $spans = $spanProcessor->endedSpans();
        static::assertEmpty($spans);
    }

    public function test_metrics_are_collected_for_stream_operations(): void
    {
        $metricProcessor = memory_metric_processor(void_exporter());
        $spanProcessor = memory_span_processor(void_exporter());
        [$fs, $telemetry] = FilesystemTelemetryConfigMother::createTraceableFilesystemWithTelemetry(
            $spanProcessor,
            null,
            $metricProcessor,
        );

        $testFile = path(__DIR__ . '/var/metrics_test.txt');
        $content = 'Test content for metrics';

        $writeStream = $fs->writeTo($testFile);
        $writeStream->append($content);
        $writeStream->append($content);
        $writeStream->close();

        $readStream = $fs->readFrom($testFile);
        $readStream->content();
        $readStream->close();

        $telemetry->flush();

        $metrics = $metricProcessor->metrics();
        $metricNames = \array_map(static fn($m) => $m->name, $metrics);

        static::assertContains('write_size', $metricNames);
        static::assertContains('write_operations', $metricNames);
        static::assertContains('read_size', $metricNames);
        static::assertContains('read_operations', $metricNames);
    }

    public function test_multiple_appends_create_single_span_with_cumulative_metrics(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $fs = FilesystemTelemetryConfigMother::createTraceableFilesystem($spanProcessor);

        $testFile = path(__DIR__ . '/var/multiple_appends.txt');
        $chunk = 'data chunk;';

        $writeStream = $fs->writeTo($testFile);

        for ($i = 0; $i < 10; $i++) {
            $writeStream->append($chunk);
        }
        $writeStream->close();

        $spans = $spanProcessor->endedSpans();

        static::assertCount(1, $spans);

        $destinationSpan = $spans[0];
        static::assertSame('Write multiple_appends.txt', $destinationSpan->name());
        static::assertSame(
            \strlen($chunk) * 10,
            $destinationSpan->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_WRITTEN],
        );
    }

    public function test_mv_operation_does_not_create_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $fs = FilesystemTelemetryConfigMother::createTraceableFilesystem($spanProcessor);

        $testDir = __DIR__ . '/var';
        $sourceFile = path($testDir . '/source.txt');
        $destFile = path($testDir . '/dest.txt');

        $fs->writeTo($sourceFile)->append('content')->close();
        $spanProcessor->reset();

        $result = $fs->mv($sourceFile, $destFile);

        static::assertTrue($result);

        $spans = $spanProcessor->endedSpans();
        static::assertEmpty($spans);
    }

    public function test_read_lines_tracks_bytes_in_lifecycle_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $fs = FilesystemTelemetryConfigMother::createTraceableFilesystem($spanProcessor);

        $testFile = path(__DIR__ . '/var/lines_test.txt');
        $content = "Line 1\nLine 2\nLine 3";

        $fs->writeTo($testFile)->append($content)->close();
        $spanProcessor->reset();

        $readStream = $fs->readFrom($testFile);
        $lines = \iterator_to_array($readStream->readLines());
        $readStream->close();

        static::assertCount(3, $lines);

        $spans = $spanProcessor->endedSpans();
        $sourceSpans = \array_values(\array_filter(
            $spans,
            static fn($span) => $span->name() === 'Read lines_test.txt',
        ));

        static::assertCount(1, $sourceSpans);
        $status = $sourceSpans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isOk());
    }

    public function test_rm_operation_does_not_create_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $fs = FilesystemTelemetryConfigMother::createTraceableFilesystem($spanProcessor);

        $testFile = path(__DIR__ . '/var/to_remove.txt');
        $fs->writeTo($testFile)->append('content')->close();
        $spanProcessor->reset();

        $result = $fs->rm($testFile);

        static::assertTrue($result);

        $spans = $spanProcessor->endedSpans();
        static::assertEmpty($spans);
    }

    public function test_status_operation_does_not_create_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $fs = FilesystemTelemetryConfigMother::createTraceableFilesystem($spanProcessor);

        $testFile = path(__DIR__ . '/var/status_test.txt');
        $fs->writeTo($testFile)->append('content')->close();
        $spanProcessor->reset();

        $status = $fs->status($testFile);

        static::assertNotNull($status);
        static::assertTrue($status->isFile());

        $spans = $spanProcessor->endedSpans();
        static::assertEmpty($spans);
    }

    public function test_stream_lifecycle_tracing_can_be_disabled(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $fs = FilesystemTelemetryConfigMother::createTraceableFilesystem(
            $spanProcessor,
            filesystem_telemetry_options(traceStreams: false),
        );

        $testFile = path(__DIR__ . '/var/no_stream_trace.txt');

        $writeStream = $fs->writeTo($testFile);
        $writeStream->append('content');
        $writeStream->close();

        $spans = $spanProcessor->endedSpans();

        static::assertCount(0, $spans);
    }
}
