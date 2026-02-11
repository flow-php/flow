<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\Telemetry;

use function Flow\Filesystem\DSL\{filesystem_telemetry_config, filesystem_telemetry_options, native_local_filesystem, path};
use function Flow\Telemetry\DSL\{logger_provider, memory_context_storage, memory_log_processor, memory_metric_processor, memory_span_processor, meter_provider, resource, telemetry, tracer_provider, void_log_exporter, void_metric_exporter, void_span_exporter};
use Flow\Filesystem\Telemetry\{FilesystemTelemetryAttributes, FilesystemTelemetryOptions, TraceableFilesystem};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
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

    public function test_bytes_tracking_is_accurate() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor);

        $testFile = path($this->testDir . '/bytes_test.txt');
        $content = 'Test content for bytes tracking';

        $writeStream = $fs->writeTo($testFile);
        $writeStream->append($content);
        $writeStream->close();

        $readStream = $fs->readFrom($testFile);
        $readStream->content();
        $readStream->close();

        $spans = $spanProcessor->endedSpans();

        $appendSpan = $this->findSpanByName($spans, 'DestinationStream::append');
        $contentSpan = $this->findSpanByName($spans, 'SourceStream::content');

        self::assertNotNull($appendSpan);
        self::assertNotNull($contentSpan);

        self::assertSame(
            \strlen($content),
            $appendSpan->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_WRITTEN]
        );
        self::assertSame(
            \strlen($content),
            $contentSpan->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_READ]
        );
    }

    public function test_complete_read_write_workflow_produces_expected_spans() : void
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

        self::assertContains('Filesystem::writeTo', $spanNames);
        self::assertContains('DestinationStream::append', $spanNames);
        self::assertContains('DestinationStream::close', $spanNames);
        self::assertContains('Filesystem::readFrom', $spanNames);
        self::assertContains('SourceStream::content', $spanNames);
        self::assertContains('SourceStream::close', $spanNames);

        foreach ($spans as $span) {
            self::assertNotNull($span->status());
            self::assertTrue($span->status()->isOk());
        }
    }

    public function test_filesystem_operations_can_be_disabled_independently() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor, filesystem_telemetry_options(
            traceFilesystemOperations: false,
            traceStreamOperations: true,
        ));

        $testFile = path($this->testDir . '/no_fs_trace.txt');

        $writeStream = $fs->writeTo($testFile);
        $writeStream->append('content');
        $writeStream->close();

        $spans = $spanProcessor->endedSpans();
        $spanNames = \array_map(static fn ($span) => $span->name(), $spans);

        self::assertNotContains('Filesystem::writeTo', $spanNames);
        self::assertContains('DestinationStream::append', $spanNames);
        self::assertContains('DestinationStream::close', $spanNames);
    }

    public function test_from_resource_creates_span() : void
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
        $fromResourceSpan = $this->findSpanByName($spans, 'DestinationStream::fromResource');

        self::assertNotNull($fromResourceSpan);
        self::assertNotNull($fromResourceSpan->status());
        self::assertTrue($fromResourceSpan->status()->isOk());
        self::assertSame('destination', $fromResourceSpan->attributes()[FilesystemTelemetryAttributes::ATTR_STREAM_TYPE]);
    }

    public function test_iterate_tracks_total_bytes_read() : void
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
        $iterateSpan = $this->findSpanByName($spans, 'SourceStream::iterate');

        self::assertNotNull($iterateSpan);
        self::assertSame(
            \strlen($content),
            $iterateSpan->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_READ]
        );
    }

    public function test_list_operation_creates_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor);

        $fs->writeTo(path($this->testDir . '/file1.txt'))->append('content1')->close();
        $fs->writeTo(path($this->testDir . '/file2.txt'))->append('content2')->close();
        $spanProcessor->reset();

        $files = \iterator_to_array($fs->list(path($this->testDir . '/*.txt')));

        self::assertCount(2, $files);

        $spans = $spanProcessor->endedSpans();
        $listSpan = $this->findSpanByName($spans, 'Filesystem::list');

        self::assertNotNull($listSpan);
        self::assertTrue($listSpan->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_IS_PATTERN]);
        self::assertNotNull($listSpan->status());
        self::assertTrue($listSpan->status()->isOk());
    }

    public function test_mv_operation_creates_span_with_source_and_destination() : void
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
        $mvSpan = $this->findSpanByName($spans, 'Filesystem::mv');

        self::assertNotNull($mvSpan);
        self::assertSame($sourceFile->uri(), $mvSpan->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_FROM]);
        self::assertSame($destFile->uri(), $mvSpan->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_TO]);
        self::assertSame('mv', $mvSpan->attributes()[FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION]);
    }

    public function test_protocol_attribute_is_set_correctly() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor);

        $testFile = path($this->testDir . '/protocol_test.txt');
        $fs->writeTo($testFile)->append('content')->close();

        $spans = $spanProcessor->endedSpans();
        $writeToSpan = $this->findSpanByName($spans, 'Filesystem::writeTo');

        self::assertNotNull($writeToSpan);
        self::assertSame('file', $writeToSpan->attributes()[FilesystemTelemetryAttributes::ATTR_FILESYSTEM_PROTOCOL]);
    }

    public function test_read_lines_tracks_bytes_read() : void
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
        $readLinesSpan = $this->findSpanByName($spans, 'SourceStream::readLines');

        self::assertNotNull($readLinesSpan);
        self::assertNotNull($readLinesSpan->status());
        self::assertTrue($readLinesSpan->status()->isOk());
    }

    public function test_rm_operation_creates_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor);

        $testFile = path($this->testDir . '/to_remove.txt');
        $fs->writeTo($testFile)->append('content')->close();
        $spanProcessor->reset();

        $result = $fs->rm($testFile);

        self::assertTrue($result);

        $spans = $spanProcessor->endedSpans();
        $rmSpan = $this->findSpanByName($spans, 'Filesystem::rm');

        self::assertNotNull($rmSpan);
        self::assertSame('rm', $rmSpan->attributes()[FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION]);
        self::assertSame($testFile->uri(), $rmSpan->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_URI]);
    }

    public function test_status_operation_creates_span() : void
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
        $statusSpan = $this->findSpanByName($spans, 'Filesystem::status');

        self::assertNotNull($statusSpan);
        self::assertSame('status', $statusSpan->attributes()[FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION]);
    }

    public function test_stream_operations_can_be_disabled_independently() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $fs = $this->createTraceableFilesystem($spanProcessor, filesystem_telemetry_options(
            traceFilesystemOperations: true,
            traceStreamOperations: false,
        ));

        $testFile = path($this->testDir . '/no_stream_trace.txt');

        $writeStream = $fs->writeTo($testFile);
        $writeStream->append('content');
        $writeStream->close();

        $spans = $spanProcessor->endedSpans();
        $spanNames = \array_map(static fn ($span) => $span->name(), $spans);

        self::assertContains('Filesystem::writeTo', $spanNames);
        self::assertNotContains('DestinationStream::append', $spanNames);
        self::assertNotContains('DestinationStream::close', $spanNames);
    }

    private function createTraceableFilesystem(MemorySpanProcessor $spanProcessor, ?FilesystemTelemetryOptions $options = null) : TraceableFilesystem
    {
        $clock = new SystemClock();
        $contextStorage = memory_context_storage();

        $tel = telemetry(
            resource(),
            tracer_provider($spanProcessor, $clock, $contextStorage),
            meter_provider(memory_metric_processor(void_metric_exporter()), $clock),
            logger_provider(memory_log_processor(void_log_exporter()), $clock, $contextStorage),
        );

        $config = filesystem_telemetry_config($tel, $options ?? filesystem_telemetry_options());

        return new TraceableFilesystem(native_local_filesystem(), $config);
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
