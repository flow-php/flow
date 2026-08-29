<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;
use phpseclib3\Net\SFTP;

use function count;
use function explode;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function max;
use function str_contains;
use function strlen;
use function strpos;
use function substr;
use function substr_count;

final class SFTPSourceStream implements SourceStream
{
    private readonly RemotePath $remotePath;

    private readonly SFTPSession $session;

    private ?int $size = null;

    public function __construct(
        private readonly Path $path,
        private readonly SFTP $sftp,
        private readonly Options $options = new Options(),
    ) {
        $this->remotePath = RemotePath::from($path);
        $this->session = new SFTPSession($sftp);
    }

    public function close(): void {}

    public function content(): string
    {
        return $this->download($this->sftp->get($this->remotePath->toString()));
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

        return $this->download($this->sftp->get($this->remotePath->toString(), false, $offset, $length));
    }

    /**
     * @param null|int<1, max> $length
     * @return Generator<int, string>
     */
    public function readLines(string $separator = "\n", ?int $length = null): Generator
    {
        $chunkSize = $length ?? $this->options->readChunkSize();
        $separatorLength = strlen($separator);
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

            if (substr_count($content, $separator) > 1) {
                $lines = explode($separator, $content);
                $lastIndex = count($lines) - 1;

                for ($i = 0; $i < $lastIndex; $i++) {
                    yield $lines[$i];
                }

                $content = $lines[$lastIndex];

                continue;
            }

            $position = strpos($content, $separator);

            if ($position !== false) {
                yield substr($content, 0, $position);
                $content = substr($content, $position + $separatorLength);
            }
        }

        if ($content !== '') {
            yield $content;
        }
    }

    public function size(): ?int
    {
        if ($this->size === null) {
            $size = self::narrowSize($this->sftp->filesize($this->remotePath->toString()));

            if ($size === null) {
                $this->session->assertAlive('read the size of ' . $this->remotePath->toString());

                return null;
            }

            $this->size = $size;
        }

        return $this->size;
    }

    private static function narrowSize(mixed $filesize): ?int
    {
        return type_integer()->isValid($filesize) ? $filesize : null;
    }

    private function download(mixed $content): string
    {
        if (type_string()->isValid($content)) {
            return $content;
        }

        $this->session->assertAlive('read ' . $this->remotePath->toString());

        return '';
    }
}
