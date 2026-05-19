<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Integration;

use FilesystemIterator;
use Flow\Bridge\Telemetry\OTLP\Transport\StreamTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\TransportException;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Signal\SignalType;
use Flow\Telemetry\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Tests\Mother\MetricMother;
use Flow\Telemetry\Tests\Mother\SpanMother;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;

use function array_filter;
use function array_values;
use function bin2hex;
use function chmod;
use function dirname;
use function explode;
use function file_get_contents;
use function fileperms;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_exporter;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_stream_transport;
use function is_dir;
use function json_decode;
use function mkdir;
use function random_bytes;
use function rewind;
use function rmdir;
use function rtrim;
use function sprintf;
use function str_repeat;
use function stream_get_contents;
use function strlen;
use function strtolower;
use function substr;
use function substr_count;
use function sys_get_temp_dir;
use function touch;
use function unlink;

use const JSON_THROW_ON_ERROR;

/**
 * End-to-end test for OTLP stream export through OTLPExporter + StreamTransport.
 *
 * Covers both file-path destinations (real filesystem boundary) and php://temp
 * memory streams. No OTel collector required.
 */
final class StreamExportIntegrationTest extends TestCase
{
    private string $tempDir;

    protected function setUp(): void
    {
        $this->tempDir = sys_get_temp_dir() . '/flow-otlp-stream-integration-' . bin2hex(random_bytes(6));
        mkdir($this->tempDir, 0755, true);
    }

    protected function tearDown(): void
    {
        if (!is_dir($this->tempDir)) {
            return;
        }

        foreach (new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($this->tempDir, FilesystemIterator::SKIP_DOTS),
            RecursiveIteratorIterator::CHILD_FIRST,
        ) as $entry) {
            /** @var \SplFileInfo $entry */
            if ($entry->isDir()) {
                rmdir($entry->getPathname());
            } else {
                unlink($entry->getPathname());
            }
        }

        rmdir($this->tempDir);
    }

    public function test_appends_multiple_batches_to_file_as_separate_lines(): void
    {
        $path = $this->tempDir . '/logs.jsonl';
        $exporter = otlp_exporter(otlp_stream_transport($path));

        static::assertTrue($exporter->export(Signals::logs([LogEntryMother::deterministic('first', Severity::INFO)])));
        static::assertTrue($exporter->export(Signals::logs([LogEntryMother::deterministic('second', Severity::WARN)])));

        $lines = array_values(array_filter(
            explode("\n", (string) file_get_contents($path)),
            static fn(string $l): bool => $l !== '',
        ));

        static::assertCount(2, $lines);

        foreach ($lines as $line) {
            static::assertJson($line);
            /** @var array<string, mixed> $decoded */
            $decoded = json_decode($line, true, flags: JSON_THROW_ON_ERROR);
            static::assertArrayHasKey('resourceLogs', $decoded);
        }
    }

    public function test_applies_file_permissions_to_newly_created_file(): void
    {
        $path = $this->tempDir . '/perms.jsonl';
        new StreamTransport($path, filePermissions: 0600);

        static::assertSame('0600', substr(sprintf('%o', fileperms($path)), -4));
    }

    public function test_concurrent_writes_to_shared_stream_interleave_at_line_boundaries(): void
    {
        $transportA = otlp_stream_transport('php://temp');
        $transportB = otlp_stream_transport('php://temp');

        $exporterA = otlp_exporter($transportA);
        $exporterB = otlp_exporter($transportB);

        for ($i = 0; $i < 10; $i++) {
            static::assertTrue($exporterA->export(Signals::logs([LogEntryMother::deterministic(
                "a-{$i}",
                Severity::INFO,
            )])));
            static::assertTrue($exporterB->export(Signals::logs([LogEntryMother::deterministic(
                "b-{$i}",
                Severity::INFO,
            )])));
        }

        static::assertInstanceOf(StreamTransport::class, $transportA);
        static::assertInstanceOf(StreamTransport::class, $transportB);

        foreach ([$transportA, $transportB] as $transport) {
            rewind($transport->stream());
            $lines = array_values(array_filter(
                explode("\n", (string) stream_get_contents($transport->stream())),
                static fn(string $l): bool => $l !== '',
            ));

            static::assertCount(10, $lines);

            foreach ($lines as $line) {
                static::assertJson($line);
            }
        }
    }

    public function test_creates_parent_directory_when_enabled(): void
    {
        $path = $this->tempDir . '/nested/deeply/logs.jsonl';
        new StreamTransport($path);

        static::assertDirectoryExists(dirname($path));
    }

    public function test_does_not_chmod_existing_file(): void
    {
        $path = $this->tempDir . '/preexisting.jsonl';
        touch($path);
        chmod($path, 0640);

        new StreamTransport($path, filePermissions: 0600);

        static::assertSame('0640', substr(sprintf('%o', fileperms($path)), -4));
    }

    public function test_does_not_create_parent_directory_when_disabled(): void
    {
        $this->expectException(TransportException::class);

        new StreamTransport($this->tempDir . '/missing-dir/logs.jsonl', createDirectories: false);
    }

    public function test_large_batch_above_default_chunk_size_is_written_intact(): void
    {
        $transport = otlp_stream_transport('php://temp');
        $exporter = otlp_exporter($transport);

        $entries = [];

        for ($i = 0; $i < 200; $i++) {
            $entries[] = LogEntryMother::deterministic(str_repeat('x', 256) . ' #' . $i, Severity::INFO);
        }

        static::assertTrue($exporter->export(Signals::logs($entries)));

        static::assertInstanceOf(StreamTransport::class, $transport);
        rewind($transport->stream());

        $contents = (string) stream_get_contents($transport->stream());

        static::assertGreaterThan(50_000, strlen($contents));
        static::assertStringEndsWith("\n", $contents);
        static::assertSame(1, substr_count($contents, "\n"));

        /** @var array{resourceLogs: list<array{scopeLogs: list<array{logRecords: list<mixed>}>}>} $decoded */
        $decoded = json_decode(rtrim($contents, "\n"), true, flags: JSON_THROW_ON_ERROR);
        static::assertArrayHasKey('resourceLogs', $decoded);
        static::assertCount(200, $decoded['resourceLogs'][0]['scopeLogs'][0]['logRecords']);
    }

    #[TestWith(['file'])]
    #[TestWith(['stream'])]
    public function test_logs_metrics_and_spans_export_to_separate_destinations(string $kind): void
    {
        $readLines = static function (StreamTransport $transport, string $destination) use ($kind): array {
            if ($kind === 'file') {
                $contents = (string) file_get_contents($destination);
            } else {
                rewind($transport->stream());
                $contents = (string) stream_get_contents($transport->stream());
            }

            return array_values(array_filter(explode("\n", $contents), static fn(string $l): bool => $l !== ''));
        };

        $logsDest = $kind === 'file' ? $this->tempDir . '/logs.jsonl' : 'php://temp';
        $metricsDest = $kind === 'file' ? $this->tempDir . '/metrics.jsonl' : 'php://temp';
        $tracesDest = $kind === 'file' ? $this->tempDir . '/traces.jsonl' : 'php://temp';

        $logsTransport = otlp_stream_transport($logsDest);
        $metricsTransport = otlp_stream_transport($metricsDest);
        $tracesTransport = otlp_stream_transport($tracesDest);

        $logsExporter = otlp_exporter($logsTransport);
        $metricsExporter = otlp_exporter($metricsTransport);
        $tracesExporter = otlp_exporter($tracesTransport);

        static::assertTrue($logsExporter->export(Signals::logs([
            LogEntryMother::deterministic('hello', Severity::INFO),
            LogEntryMother::deterministic('world', Severity::WARN),
        ])));
        static::assertTrue($logsExporter->export(Signals::logs([
            LogEntryMother::deterministic('again', Severity::ERROR),
        ])));

        static::assertTrue($metricsExporter->export(Signals::metrics([
            MetricMother::deterministicCounter('requests.count', 7),
        ])));
        static::assertTrue($tracesExporter->export(Signals::traces([
            SpanMother::withName('checkout'),
        ])));

        static::assertInstanceOf(StreamTransport::class, $logsTransport);
        static::assertInstanceOf(StreamTransport::class, $metricsTransport);
        static::assertInstanceOf(StreamTransport::class, $tracesTransport);

        foreach ([
            [$logsTransport,    $logsDest,    'resourceLogs',    2],
            [$metricsTransport, $metricsDest, 'resourceMetrics', 1],
            [$tracesTransport,  $tracesDest,  'resourceSpans',   1],
        ] as [$transport, $destination, $key, $expectedCount]) {
            $lines = $readLines($transport, $destination);

            static::assertCount($expectedCount, $lines);

            foreach ($lines as $line) {
                static::assertJson($line);
                /** @var array<string, mixed> $decoded */
                $decoded = json_decode($line, true, flags: JSON_THROW_ON_ERROR);
                static::assertArrayHasKey($key, $decoded);
            }
        }
    }

    #[TestWith(['file'])]
    #[TestWith(['stream'])]
    public function test_mixed_signal_destination_appends_one_line_per_signal_type(string $kind): void
    {
        $destination = $kind === 'file' ? $this->tempDir . '/all-signals.jsonl' : 'php://temp';

        $transport = otlp_stream_transport($destination);
        $exporter = otlp_exporter($transport);

        static::assertTrue($exporter->export(Signals::logs([LogEntryMother::deterministic('hello', Severity::INFO)])));
        static::assertTrue($exporter->export(Signals::metrics([MetricMother::deterministicCounter('m', 1)])));
        static::assertTrue($exporter->export(Signals::traces([SpanMother::withName('s')])));

        static::assertInstanceOf(StreamTransport::class, $transport);

        if ($kind === 'file') {
            $contents = (string) file_get_contents($destination);
        } else {
            rewind($transport->stream());
            $contents = (string) stream_get_contents($transport->stream());
        }

        $lines = array_values(array_filter(explode("\n", $contents), static fn(string $l): bool => $l !== ''));

        static::assertCount(3, $lines);

        foreach ([0 => 'resourceLogs', 1 => 'resourceMetrics', 2 => 'resourceSpans'] as $index => $expectedKey) {
            /** @var array<string, mixed> $decoded */
            $decoded = json_decode($lines[$index], true, flags: JSON_THROW_ON_ERROR);
            static::assertArrayHasKey($expectedKey, $decoded);
        }
    }

    #[TestWith([SignalType::LOGS, 'resourceLogs'])]
    #[TestWith([SignalType::METRICS, 'resourceMetrics'])]
    #[TestWith([SignalType::TRACES, 'resourceSpans'])]
    public function test_writes_each_signal_type_to_file_as_single_line_with_trailing_newline(
        SignalType $type,
        string $expectedKey,
    ): void {
        $path = $this->tempDir . '/' . strtolower($type->name) . '.jsonl';
        $exporter = otlp_exporter(otlp_stream_transport($path));

        static::assertTrue($exporter->export(match ($type) {
            SignalType::LOGS => Signals::logs([LogEntryMother::deterministic('hello', Severity::INFO)]),
            SignalType::METRICS => Signals::metrics([MetricMother::deterministicCounter('test.counter', 42)]),
            SignalType::TRACES => Signals::traces([SpanMother::withName('span')]),
        }));

        $contents = (string) file_get_contents($path);

        static::assertStringEndsWith("\n", $contents);
        static::assertSame(1, substr_count($contents, "\n"));

        /** @var array<string, mixed> $decoded */
        $decoded = json_decode(rtrim($contents, "\n"), true, flags: JSON_THROW_ON_ERROR);
        static::assertArrayHasKey($expectedKey, $decoded);
    }
}
