<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

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
