<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\Transport;

use Flow\Bridge\Telemetry\OTLP\Transport\StreamTransport;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Signal\{SignalType, Signals};
use Flow\Telemetry\Tests\Mother\{LogEntryMother, MetricMother, SpanMother};
use Flow\Telemetry\Transport\TransportException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class StreamTransportTest extends TestCase
{
    #[TestWith(['php://stdout'])]
    #[TestWith(['php://stderr'])]
    #[TestWith(['php://memory'])]
    #[TestWith(['php://temp'])]
    #[TestWith(['php://temp/maxmemory:1024'])]
    public function test_accepts_php_stream_wrappers(string $uri) : void
    {
        $transport = new StreamTransport($uri);

        self::assertIsResource($transport->stream());
    }

    public function test_appends_multiple_batches_as_separate_lines() : void
    {
        $transport = new StreamTransport('php://memory');

        $transport->send(Signals::logs([LogEntryMother::deterministic('first', Severity::INFO)]));
        $transport->send(Signals::logs([LogEntryMother::deterministic('second', Severity::WARN)]));

        \rewind($transport->stream());
        $contents = (string) \stream_get_contents($transport->stream());
        $lines = \array_values(\array_filter(\explode("\n", $contents), static fn (string $l) : bool => $l !== ''));

        self::assertCount(2, $lines);
    }

    public function test_constructor_sets_stream_chunk_size() : void
    {
        $transport = new StreamTransport('php://memory');

        $previous = \stream_set_chunk_size($transport->stream(), 8192);

        self::assertSame(StreamTransport::STREAM_CHUNK_SIZE, $previous);
    }

    public function test_empty_destination_throws() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('non-empty');

        new StreamTransport('');
    }

    public function test_empty_signal_does_not_write() : void
    {
        $transport = new StreamTransport('php://memory');

        $transport->send(Signals::logs([]));
        $transport->send(Signals::metrics([]));
        $transport->send(Signals::traces([]));

        \rewind($transport->stream());
        self::assertSame('', (string) \stream_get_contents($transport->stream()));
    }

    #[TestWith([-1])]
    #[TestWith([01000])]
    public function test_out_of_range_file_permissions_throw(int $perm) : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('between 0 and 0777');

        new StreamTransport('php://memory', filePermissions: $perm);
    }

    public function test_send_after_shutdown_throws() : void
    {
        $transport = new StreamTransport('php://memory');

        $transport->shutdown();

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('Cannot send after shutdown');

        $transport->send(Signals::logs([LogEntryMother::deterministic('hello', Severity::INFO)]));
    }

    public function test_shutdown_is_idempotent() : void
    {
        $transport = new StreamTransport('php://memory');

        $transport->shutdown();
        $transport->shutdown();

        $this->expectException(TransportException::class);
        $transport->send(Signals::logs([LogEntryMother::deterministic('hello', Severity::INFO)]));
    }

    #[TestWith([SignalType::LOGS, 'resourceLogs'])]
    #[TestWith([SignalType::METRICS, 'resourceMetrics'])]
    #[TestWith([SignalType::TRACES, 'resourceSpans'])]
    public function test_writes_each_signal_type_as_single_line_with_trailing_newline(SignalType $type, string $expectedKey) : void
    {
        $transport = new StreamTransport('php://memory');

        $transport->send(match ($type) {
            SignalType::LOGS => Signals::logs([LogEntryMother::deterministic('hello', Severity::INFO)]),
            SignalType::METRICS => Signals::metrics([MetricMother::deterministicCounter('test.counter', 42)]),
            SignalType::TRACES => Signals::traces([SpanMother::withName('span')]),
        });

        \rewind($transport->stream());
        $contents = (string) \stream_get_contents($transport->stream());

        self::assertStringEndsWith("\n", $contents);
        self::assertSame(1, \substr_count($contents, "\n"));

        /** @var array<string, mixed> $decoded */
        $decoded = \json_decode(\rtrim($contents, "\n"), true, flags: \JSON_THROW_ON_ERROR);
        self::assertArrayHasKey($expectedKey, $decoded);
    }
}
