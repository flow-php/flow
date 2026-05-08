<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Transport;

use Flow\Bridge\Telemetry\OTLP\Serializer\JsonSerializer;
use Flow\Telemetry\Signal\{SignalType, Signals};

/**
 * OTLP File Exporter transport — writes JSONL to either a file path or a php://
 * stream wrapper. The handle is opened once in the constructor and reused
 * across send() calls; shutdown() closes it.
 *
 * Each send() acquires LOCK_EX around fwrite so concurrent writers (e.g. forked
 * workers sharing a redirected stdout/stderr or appending to the same log file)
 * interleave at line boundaries. flock() on TTYs and pipes returns false
 * silently — the call is best-effort and never blocks the write.
 *
 * For file paths the parent directory is created on construction when
 * $createDirectories is true, and the file mode is set to $filePermissions if
 * the file is newly created. Both options are ignored for php:// URIs.
 *
 * @see https://opentelemetry.io/docs/specs/otel/protocol/file-exporter/
 */
final class StreamTransport implements Transport
{
    public const int STREAM_CHUNK_SIZE = 1_000_000;

    private ?string $errorMessage = null;

    private bool $isShutdown = false;

    private readonly JsonSerializer $serializer;

    /** @var resource */
    private $stream;

    public function __construct(
        private readonly string $destination,
        int $filePermissions = 0644,
        bool $createDirectories = true,
    ) {
        if ($destination === '') {
            throw new \InvalidArgumentException('StreamTransport destination must be a non-empty string');
        }

        if ($filePermissions < 0 || $filePermissions > 0777) {
            throw new \InvalidArgumentException('File permissions must be between 0 and 0777');
        }

        $isStreamWrapper = \str_starts_with($destination, 'php://');
        $existedBefore = !$isStreamWrapper && \is_file($destination);

        if (!$isStreamWrapper && $createDirectories) {
            $directory = \dirname($destination);

            if (!\is_dir($directory)) {
                $created = $this->captureError(static fn () : bool => \mkdir($directory, 0755, true));

                if ($created === false && !\is_dir($directory)) {
                    throw new TransportException(\sprintf(
                        'Failed to create directory "%s" for StreamTransport destination: %s',
                        $directory,
                        $this->errorMessage ?? 'unknown error',
                    ));
                }
            }
        }

        $handle = $this->captureError(static fn () => \fopen($destination, 'a+b'));

        if (!\is_resource($handle)) {
            throw new TransportException(\sprintf(
                'Failed to open OTLP stream "%s": %s',
                $destination,
                $this->errorMessage ?? 'unknown error',
            ));
        }

        $this->stream = $handle;
        \stream_set_chunk_size($this->stream, self::STREAM_CHUNK_SIZE);
        $this->serializer = new JsonSerializer();

        if (!$isStreamWrapper && !$existedBefore) {
            @\chmod($destination, $filePermissions);
        }
    }

    public function send(Signals $signal) : void
    {
        if ($this->isShutdown) {
            throw new TransportException('Cannot send after shutdown');
        }

        if ($signal->count() === 0) {
            return;
        }

        $payload = match ($signal->type) {
            SignalType::LOGS => $this->serializer->serializeLogs($signal->allLogs()),
            SignalType::METRICS => $this->serializer->serializeMetrics($signal->allMetrics()),
            SignalType::TRACES => $this->serializer->serializeSpans($signal->allSpans()),
        } . "\n";

        $stream = $this->stream;

        @\flock($stream, \LOCK_EX);

        try {
            $written = $this->captureError(static fn () : false|int => \fwrite($stream, $payload));
        } finally {
            @\flock($stream, \LOCK_UN);
        }

        if (!\is_int($written)) {
            throw new TransportException(\sprintf(
                'Failed to write OTLP payload to "%s": %s',
                $this->destination,
                $this->errorMessage ?? 'unknown error',
            ));
        }

        if ($written < \strlen($payload)) {
            throw new TransportException(\sprintf(
                'Partial write to OTLP stream "%s": wrote %d of %d bytes',
                $this->destination,
                $written,
                \strlen($payload),
            ));
        }
    }

    public function shutdown() : void
    {
        if ($this->isShutdown) {
            return;
        }

        @\fclose($this->stream);
        $this->isShutdown = true;
    }

    /**
     * @return resource
     */
    public function stream()
    {
        return $this->stream;
    }

    private function captureError(\Closure $operation) : mixed
    {
        $this->errorMessage = null;
        \set_error_handler(function (int $code, string $message) : bool {
            $this->errorMessage = \preg_replace('{^\w+\(.*?\): }', '', $message);

            return true;
        });

        try {
            return $operation();
        } finally {
            \restore_error_handler();
        }
    }
}
