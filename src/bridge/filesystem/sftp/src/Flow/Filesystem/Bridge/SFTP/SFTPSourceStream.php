<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;
use phpseclib3\Net\SFTP;

use function array_pop;
use function explode;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function max;
use function str_contains;
use function strlen;

final class SFTPSourceStream implements SourceStream
{
    private readonly string $remotePath;

    private ?int $size = null;

    public function __construct(
        private readonly Path $path,
        private readonly SFTP $sftp,
        private readonly Options $options = new Options(),
    ) {
        $this->remotePath = $path->path();
    }

    public function close(): void {}

    public function content(): string
    {
        $content = $this->sftp->get($this->remotePath);

        if ($content === false) {
            $this->sftp->isConnected() && $this->sftp->isAuthenticated()
                || throw new RuntimeException('SFTP session is no longer usable, cannot read ' . $this->remotePath);
        }

        return type_string()->assert($content);
    }

    public function isOpen(): bool
    {
        return true;
    }

    /**
     * @param int<1, max> $length
     * @return Generator<int, string>
     */
    public function iterate(int $length = 1): Generator
    {
        $size = $this->size() ?? 0;

        for ($offset = 0; $offset < $size; $offset += $length) {
            yield $this->read($length, $offset);
        }
    }

    public function path(): Path
    {
        return $this->path;
    }

    public function read(int $length, int $offset): string
    {
        if ($offset < 0) {
            $offset = max(0, ($this->size() ?? 0) + $offset);
        }

        $content = $this->sftp->get($this->remotePath, false, $offset, $length);

        if ($content === false) {
            $this->sftp->isConnected() && $this->sftp->isAuthenticated()
                || throw new RuntimeException('SFTP session is no longer usable, cannot read ' . $this->remotePath);
        }

        return type_string()->assert($content);
    }

    /**
     * @param null|int<1, max> $length
     * @return Generator<int, string>
     */
    public function readLines(string $separator = "\n", ?int $length = null): Generator
    {
        $chunkSize = $length ?? $this->options->readChunkSize();
        $size = $this->size() ?? 0;
        $offset = 0;
        $content = '';

        while ($offset < $size) {
            $chunk = $this->read($chunkSize, $offset);

            if ($chunk === '') {
                break;
            }

            $offset += strlen($chunk);
            $content .= $chunk;

            if (!str_contains($content, $separator)) {
                continue;
            }

            $lines = explode($separator, $content);
            $content = array_pop($lines);

            foreach ($lines as $line) {
                yield $line;
            }
        }

        if ($content !== '') {
            yield $content;
        }
    }

    public function size(): ?int
    {
        if ($this->size === null) {
            // @mago-ignore analysis:mixed-assignment
            $filesize = $this->sftp->filesize($this->remotePath);

            if ($filesize === false) {
                $this->sftp->isConnected() && $this->sftp->isAuthenticated()
                    || throw new RuntimeException('SFTP session is no longer usable, cannot read the size of '
                    . $this->remotePath);

                return null;
            }

            $this->size = type_integer()->assert($filesize);
        }

        return $this->size;
    }
}
