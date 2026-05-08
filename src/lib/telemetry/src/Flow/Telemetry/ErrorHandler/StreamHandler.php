<?php

declare(strict_types=1);

namespace Flow\Telemetry\ErrorHandler;

/**
 * Appends formatted Throwables (one per line) to a file path or php:// stream
 * wrapper. The stream is opened lazily on the first handle() call and reused
 * across subsequent calls; __destruct() closes it.
 *
 * Concurrent writers acquire LOCK_EX around fwrite so lines never interleave;
 * flock() on TTYs and pipes returns false silently — best-effort.
 */
final class StreamHandler implements ErrorHandler
{
    /** @var null|resource */
    private $stream;

    public function __construct(
        private readonly string $destination,
        private readonly int $filePermissions = 0644,
        private readonly bool $createDirectories = true,
        private readonly string $messagePrefix = '[flow-telemetry]',
    ) {
        if ($destination === '') {
            throw new \InvalidArgumentException('StreamHandler destination must be a non-empty string');
        }

        if ($filePermissions < 0 || $filePermissions > 0777) {
            throw new \InvalidArgumentException('File permissions must be between 0 and 0777');
        }
    }

    public function __destruct()
    {
        if (\is_resource($this->stream)) {
            @\fclose($this->stream);
            $this->stream = null;
        }
    }

    public function handle(\Throwable $error) : void
    {
        try {
            $stream = $this->openStream();

            if ($stream === null) {
                return;
            }

            $payload = \sprintf(
                "%s %s: %s in %s:%d\n",
                $this->messagePrefix,
                $error::class,
                $error->getMessage(),
                $error->getFile(),
                $error->getLine(),
            );

            @\flock($stream, \LOCK_EX);

            try {
                @\fwrite($stream, $payload);
            } finally {
                @\flock($stream, \LOCK_UN);
            }
        } catch (\Throwable) {
        }
    }

    /**
     * @return null|resource
     */
    private function openStream()
    {
        if (\is_resource($this->stream)) {
            return $this->stream;
        }

        $isStreamWrapper = \str_starts_with($this->destination, 'php://');
        $existedBefore = !$isStreamWrapper && \is_file($this->destination);

        if (!$isStreamWrapper && $this->createDirectories) {
            $directory = \dirname($this->destination);

            if (!\is_dir($directory)) {
                @\mkdir($directory, 0755, true);
            }
        }

        $handle = @\fopen($this->destination, 'a+b');

        if (!\is_resource($handle)) {
            return null;
        }

        if (!$isStreamWrapper && !$existedBefore) {
            @\chmod($this->destination, $this->filePermissions);
        }

        $this->stream = $handle;

        return $this->stream;
    }
}
