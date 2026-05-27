<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\Transport;

use Flow\Bridge\Telemetry\OTLP\Transport\StreamTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\TransportException;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Signal\SignalType;
use Flow\Telemetry\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Tests\Mother\MetricMother;
use Flow\Telemetry\Tests\Mother\SpanMother;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_filter;
use function array_values;
use function explode;
use function floor;
use function getcwd;
use function is_dir;
use function json_decode;
use function rewind;
use function rmdir;
use function rtrim;
use function stream_get_contents;
use function stream_set_chunk_size;
use function stream_wrapper_register;
use function stream_wrapper_unregister;
use function strlen;
use function substr_count;

use const JSON_THROW_ON_ERROR;

final class StreamTransportTest extends TestCase
{
    #[TestWith(['php://stdout'])]
    #[TestWith(['php://stderr'])]
    #[TestWith(['php://memory'])]
    #[TestWith(['php://temp'])]
    #[TestWith(['php://temp/maxmemory:1024'])]
    public function test_accepts_php_stream_wrappers(string $uri): void
    {
        $transport = new StreamTransport($uri);

        static::assertIsResource($transport->stream());
    }

    public function test_appends_multiple_batches_as_separate_lines(): void
    {
        $transport = new StreamTransport('php://memory');

        $transport->send(Signals::logs([LogEntryMother::deterministic('first', Severity::INFO)]));
        $transport->send(Signals::logs([LogEntryMother::deterministic('second', Severity::WARN)]));

        rewind($transport->stream());
        $contents = (string) stream_get_contents($transport->stream());
        $lines = array_values(array_filter(explode("\n", $contents), static fn(string $l): bool => $l !== ''));

        static::assertCount(2, $lines);
    }

    public function test_constructor_sets_stream_chunk_size(): void
    {
        $transport = new StreamTransport('php://memory');

        $previous = stream_set_chunk_size($transport->stream(), 8192);

        static::assertSame(StreamTransport::STREAM_CHUNK_SIZE, $previous);
    }

    public function test_does_not_create_directory_for_custom_stream_wrapper_schemes(): void
    {
        stream_wrapper_register('flow-no-mkdir', PartialWriteStreamWrapper::class);
        $leakedDirectory = (getcwd() ?: '.') . '/flow-no-mkdir:';

        try {
            new StreamTransport('flow-no-mkdir://buf');

            static::assertDirectoryDoesNotExist($leakedDirectory);
        } finally {
            stream_wrapper_unregister('flow-no-mkdir');

            if (is_dir($leakedDirectory)) {
                rmdir($leakedDirectory);
            }
        }
    }

    public function test_empty_destination_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('non-empty');

        new StreamTransport('');
    }

    public function test_empty_signal_does_not_write(): void
    {
        $transport = new StreamTransport('php://memory');

        $transport->send(Signals::logs([]));
        $transport->send(Signals::metrics([]));
        $transport->send(Signals::traces([]));

        rewind($transport->stream());
        static::assertSame('', (string) stream_get_contents($transport->stream()));
    }

    public function test_failed_write_throws(): void
    {
        stream_wrapper_register('flow-failwrite', FailingWriteStreamWrapper::class);

        try {
            $transport = new StreamTransport('flow-failwrite://buf');

            $this->expectException(TransportException::class);
            $this->expectExceptionMessage('Failed to write OTLP payload to "flow-failwrite://buf"');

            $transport->send(Signals::logs([LogEntryMother::deterministic('hello', Severity::INFO)]));
        } finally {
            stream_wrapper_unregister('flow-failwrite');
        }
    }

    #[TestWith([-1])]
    #[TestWith([01000])]
    public function test_out_of_range_file_permissions_throw(int $perm): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('between 0 and 0777');

        new StreamTransport('php://memory', filePermissions: $perm);
    }

    public function test_partial_write_throws(): void
    {
        stream_wrapper_register('flow-partial', PartialWriteStreamWrapper::class);

        try {
            $transport = new StreamTransport('flow-partial://buf');

            $this->expectException(TransportException::class);
            $this->expectExceptionMessageMatches(
                '/Partial write to OTLP stream "flow-partial:\\/\\/buf": wrote \\d+ of \\d+ bytes/',
            );

            $transport->send(Signals::logs([LogEntryMother::deterministic('hello', Severity::INFO)]));
        } finally {
            stream_wrapper_unregister('flow-partial');
        }
    }

    public function test_send_after_shutdown_throws(): void
    {
        $transport = new StreamTransport('php://memory');

        $transport->shutdown();

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('Cannot send after shutdown');

        $transport->send(Signals::logs([LogEntryMother::deterministic('hello', Severity::INFO)]));
    }

    public function test_shutdown_is_idempotent(): void
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
    public function test_writes_each_signal_type_as_single_line_with_trailing_newline(
        SignalType $type,
        string $expectedKey,
    ): void {
        $transport = new StreamTransport('php://memory');

        $transport->send(match ($type) {
            SignalType::LOGS => Signals::logs([LogEntryMother::deterministic('hello', Severity::INFO)]),
            SignalType::METRICS => Signals::metrics([MetricMother::deterministicCounter('test.counter', 42)]),
            SignalType::TRACES => Signals::traces([SpanMother::withName('span')]),
        });

        rewind($transport->stream());
        $contents = (string) stream_get_contents($transport->stream());

        static::assertStringEndsWith("\n", $contents);
        static::assertSame(1, substr_count($contents, "\n"));

        /** @var array<string, mixed> $decoded */
        $decoded = json_decode(rtrim($contents, "\n"), true, flags: JSON_THROW_ON_ERROR);
        static::assertArrayHasKey($expectedKey, $decoded);
    }
}

final class PartialWriteStreamWrapper
{
    /** @var resource */
    public $context;

    public function stream_close(): void {}

    public function stream_eof(): bool
    {
        return true;
    }

    public function stream_flush(): bool
    {
        return true;
    }

    public function stream_lock(int $operation): bool
    {
        return true;
    }

    public function stream_open(string $path, string $mode, int $options, ?string &$openedPath): bool
    {
        return true;
    }

    public function stream_set_option(int $option, int $arg1, ?int $arg2): bool
    {
        return true;
    }

    /**
     * @return array<int|string, int>
     */
    public function stream_stat(): array
    {
        return [];
    }

    public function stream_write(string $data): int
    {
        return (int) floor(strlen($data) / 2);
    }

    public function url_stat(string $path, int $flags): false
    {
        return false;
    }
}

final class FailingWriteStreamWrapper
{
    /** @var resource */
    public $context;

    public function stream_close(): void {}

    public function stream_eof(): bool
    {
        return true;
    }

    public function stream_flush(): bool
    {
        return true;
    }

    public function stream_lock(int $operation): bool
    {
        return true;
    }

    public function stream_open(string $path, string $mode, int $options, ?string &$openedPath): bool
    {
        return true;
    }

    public function stream_set_option(int $option, int $arg1, ?int $arg2): bool
    {
        return true;
    }

    /**
     * @return array<int|string, int>
     */
    public function stream_stat(): array
    {
        return [];
    }

    public function stream_write(string $data): false
    {
        return false;
    }

    public function url_stat(string $path, int $flags): false
    {
        return false;
    }
}
