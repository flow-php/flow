<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\Telemetry;

use function Flow\Filesystem\DSL\{filesystem_telemetry_config, filesystem_telemetry_options, native_local_filesystem, path};
use function Flow\Telemetry\DSL\{logger_provider, memory_context_storage, memory_log_processor, memory_metric_processor, memory_span_processor, meter_provider, resource, telemetry, tracer_provider, void_log_exporter, void_metric_exporter, void_span_exporter};
use Flow\Filesystem\Telemetry\{FilesystemTelemetryAttributes, FilesystemTelemetryOptions, TraceableFilesystem};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\{MemoryLogProcessor, MemoryMetricProcessor, MemorySpanProcessor};
use Flow\Telemetry\Telemetry;
use PHPUnit\Framework\TestCase;

final class TraceableFilesystemIntegrationTest extends TestCase
{
    private string $testDir;

    protected function setUp() : void
    {
        $this->testDir = __DIR__ . '/var';

        if (!\file_exists($this->testDir)) {
            \mkdir($this->testDir, 0777, true);
        }
    }

    protected function tearDown() : void
    {
        $this->removeDirectory($this->testDir);
    }

    public function test_complete_read_write_workflow_produces_lifecycle_spans() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor);

        $testFile = path($this->testDir . '/test_file.txt');
        $content = 'Hello, World!';

        $writeStream = $fs->writeTo($testFile);
        $writeStream->append($content);
        $writeStream->close();

        $readStream = $fs->readFrom($testFile);
        $readContent = $readStream->content();
        $readStream->close();

        self::assertSame($content, $readContent);

        $spans = $spanProcessor->endedSpans();
        $spanNames = \array_map(static fn ($span) => $span->name(), $spans);

        self::assertContains('flow.filesystem.stream.write', $spanNames);
        self::assertContains('flow.filesystem.stream.read', $spanNames);
        self::assertCount(2, $spans);

        foreach ($spans as $span) {
            self::assertNotNull($span->status());
            self::assertTrue($span->status()->isOk());
        }

        $destinationSpan = $this->findSpanByName($spans, 'flow.filesystem.stream.write');
        self::assertNotNull($destinationSpan);
        self::assertSame(\strlen($content), $destinationSpan->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_WRITTEN]);

        $sourceSpan = $this->findSpanByName($spans, 'flow.filesystem.stream.read');
        self::assertNotNull($sourceSpan);
        self::assertSame(\strlen($content), $sourceSpan->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_READ]);
    }

    public function test_filesystem_operations_do_not_create_spans() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor);

        $testFile = path($this->testDir . '/no_fs_trace.txt');

        $fs->writeTo($testFile)->append('content')->close();
        $spanProcessor->reset();

        $fs->status($testFile);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(0, $spans);
    }

    public function test_from_resource_tracks_bytes_in_lifecycle_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor);

        $sourceFile = __DIR__ . '/../Fixtures/orders.csv';

        if (!\file_exists($sourceFile)) {
            self::markTestSkipped('Test fixture file not found');
        }

        $testFile = path($this->testDir . '/from_resource_test.txt');
        $resource = \fopen($sourceFile, 'rb');
        self::assertIsResource($resource);

        $writeStream = $fs->writeTo($testFile);
        $writeStream->fromResource($resource);
        $writeStream->close();

        \fclose($resource);

        $spans = $spanProcessor->endedSpans();
        $destinationSpan = $this->findSpanByName($spans, 'flow.filesystem.stream.write');

        self::assertNotNull($destinationSpan);
        self::assertNotNull($destinationSpan->status());
        self::assertTrue($destinationSpan->status()->isOk());
        self::assertSame('destination', $destinationSpan->attributes()[FilesystemTelemetryAttributes::ATTR_STREAM_TYPE]);
    }

    public function test_iterate_tracks_total_bytes_in_lifecycle_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor);

        $testFile = path($this->testDir . '/iterate_test.txt');
        $content = "Line 1\nLine 2\nLine 3";

        $fs->writeTo($testFile)->append($content)->close();
        $spanProcessor->reset();

        $readStream = $fs->readFrom($testFile);
        $chunks = [];

        foreach ($readStream->iterate(5) as $chunk) {
            $chunks[] = $chunk;
        }
        $readStream->close();

        self::assertSame($content, \implode('', $chunks));

        $spans = $spanProcessor->endedSpans();
        $sourceSpan = $this->findSpanByName($spans, 'flow.filesystem.stream.read');

        self::assertNotNull($sourceSpan);
        self::assertSame(
            \strlen($content),
            $sourceSpan->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_READ]
        );
    }

    public function test_list_operation_does_not_create_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor);

        $fs->writeTo(path($this->testDir . '/file1.txt'))->append('content1')->close();
        $fs->writeTo(path($this->testDir . '/file2.txt'))->append('content2')->close();
        $spanProcessor->reset();

        $files = \iterator_to_array($fs->list(path($this->testDir . '/*.txt')));

        self::assertCount(2, $files);

        $spans = $spanProcessor->endedSpans();
        self::assertEmpty($spans);
    }

    public function test_metrics_are_collected_for_stream_operations() : void
    {
        $metricProcessor = memory_metric_processor(void_metric_exporter());
        $spanProcessor = memory_span_processor(void_span_exporter());
        [$fs, $telemetry] = $this->createTraceableFilesystemWithTelemetry($spanProcessor, null, $metricProcessor);

        $testFile = path($this->testDir . '/metrics_test.txt');
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
        $metricNames = \array_map(static fn ($m) => $m->name, $metrics);

        self::assertContains('flow.filesystem.write.size', $metricNames);
        self::assertContains('flow.filesystem.write.operations', $metricNames);
        self::assertContains('flow.filesystem.read.size', $metricNames);
        self::assertContains('flow.filesystem.read.operations', $metricNames);
    }

    public function test_multiple_appends_create_single_span_with_cumulative_metrics() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor);

        $testFile = path($this->testDir . '/multiple_appends.txt');
        $chunk = 'data chunk;';

        $writeStream = $fs->writeTo($testFile);

        for ($i = 0; $i < 10; $i++) {
            $writeStream->append($chunk);
        }
        $writeStream->close();

        $spans = $spanProcessor->endedSpans();

        self::assertCount(1, $spans);

        $destinationSpan = $spans[0];
        self::assertSame('flow.filesystem.stream.write', $destinationSpan->name());
        self::assertSame(\strlen($chunk) * 10, $destinationSpan->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_WRITTEN]);
    }

    public function test_mv_operation_does_not_create_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor);

        $sourceFile = path($this->testDir . '/source.txt');
        $destFile = path($this->testDir . '/dest.txt');

        $fs->writeTo($sourceFile)->append('content')->close();
        $spanProcessor->reset();

        $result = $fs->mv($sourceFile, $destFile);

        self::assertTrue($result);

        $spans = $spanProcessor->endedSpans();
        self::assertEmpty($spans);
    }

    public function test_read_lines_tracks_bytes_in_lifecycle_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor);

        $testFile = path($this->testDir . '/lines_test.txt');
        $content = "Line 1\nLine 2\nLine 3";

        $fs->writeTo($testFile)->append($content)->close();
        $spanProcessor->reset();

        $readStream = $fs->readFrom($testFile);
        $lines = \iterator_to_array($readStream->readLines());
        $readStream->close();

        self::assertCount(3, $lines);

        $spans = $spanProcessor->endedSpans();
        $sourceSpan = $this->findSpanByName($spans, 'flow.filesystem.stream.read');

        self::assertNotNull($sourceSpan);
        self::assertNotNull($sourceSpan->status());
        self::assertTrue($sourceSpan->status()->isOk());
    }

    public function test_rm_operation_does_not_create_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor);

        $testFile = path($this->testDir . '/to_remove.txt');
        $fs->writeTo($testFile)->append('content')->close();
        $spanProcessor->reset();

        $result = $fs->rm($testFile);

        self::assertTrue($result);

        $spans = $spanProcessor->endedSpans();
        self::assertEmpty($spans);
    }

    public function test_status_operation_does_not_create_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor);

        $testFile = path($this->testDir . '/status_test.txt');
        $fs->writeTo($testFile)->append('content')->close();
        $spanProcessor->reset();

        $status = $fs->status($testFile);

        self::assertNotNull($status);
        self::assertTrue($status->isFile());

        $spans = $spanProcessor->endedSpans();
        self::assertEmpty($spans);
    }

    public function test_stream_lifecycle_tracing_can_be_disabled() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor, filesystem_telemetry_options(
            traceStreams: false,
        ));

        $testFile = path($this->testDir . '/no_stream_trace.txt');

        $writeStream = $fs->writeTo($testFile);
        $writeStream->append('content');
        $writeStream->close();

        $spans = $spanProcessor->endedSpans();

        self::assertCount(0, $spans);
    }

    private function createTraceableFilesystem(
        MemorySpanProcessor $spanProcessor,
        ?FilesystemTelemetryOptions $options = null,
        ?MemoryMetricProcessor $metricProcessor = null,
        ?MemoryLogProcessor $logProcessor = null,
    ) : TraceableFilesystem {
        [$fs] = $this->createTraceableFilesystemWithTelemetry($spanProcessor, $options, $metricProcessor, $logProcessor);

        return $fs;
    }

    /**
     * @return array{0: TraceableFilesystem, 1: Telemetry}
     */
    private function createTraceableFilesystemWithTelemetry(
        MemorySpanProcessor $spanProcessor,
        ?FilesystemTelemetryOptions $options = null,
        ?MemoryMetricProcessor $metricProcessor = null,
        ?MemoryLogProcessor $logProcessor = null,
    ) : array {
        $clock = new SystemClock();
        $contextStorage = memory_context_storage();

        $tel = telemetry(
            resource(),
            tracer_provider($spanProcessor, $clock, $contextStorage),
            meter_provider($metricProcessor ?? memory_metric_processor(void_metric_exporter()), $clock),
            logger_provider($logProcessor ?? memory_log_processor(void_log_exporter()), $clock, $contextStorage),
        );

        $config = filesystem_telemetry_config($tel, $clock, $options ?? filesystem_telemetry_options());

        return [new TraceableFilesystem(native_local_filesystem(), $config), $tel];
    }

    /**
     * @param array<\Flow\Telemetry\Tracer\Span> $spans
     */
    private function findSpanByName(array $spans, string $name) : ?\Flow\Telemetry\Tracer\Span
    {
        foreach ($spans as $span) {
            if ($span->name() === $name) {
                return $span;
            }
        }

        return null;
    }

    private function removeDirectory(string $dir) : void
    {
        if (!\file_exists($dir)) {
            return;
        }

        $files = \array_diff(\scandir($dir), ['.', '..']);

        foreach ($files as $file) {
            $path = $dir . '/' . $file;

            if (\is_dir($path)) {
                $this->removeDirectory($path);
            } else {
                \unlink($path);
            }
        }

        \rmdir($dir);
    }
}
