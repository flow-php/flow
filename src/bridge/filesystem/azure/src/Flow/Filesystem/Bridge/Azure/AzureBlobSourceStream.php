<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure;

use Flow\Azure\SDK\BlobService\GetBlob\GetBlobOptions;
use Flow\Azure\SDK\BlobService\GetBlob\Range;
use Flow\Azure\SDK\BlobService\GetBlobProperties\BlobProperties;
use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;

use function array_pop;
use function explode;
use function str_contains;
use function strlen;

final class AzureBlobSourceStream implements SourceStream
{
    private ?BlobProperties $blobProperties = null;

    public function __construct(
        private readonly Path $path,
        private readonly BlobServiceInterface $blobService,
    ) {}

    public function close(): void
    {
        // do nothing as we can't close Azure Blob since we are just reading parts of it at once
    }

    public function content(): string
    {
        return $this->blobService->getBlob($this->path->path())->content();
    }

    public function isOpen(): bool
    {
        return true;
    }

    public function iterate(int $length = 1): Generator
    {
        $offset = 0;
        $size = $this->size() ?? 0;

        while ($offset < $size) {
            yield $this->read($length, $offset);
            $offset += $length;
        }
    }

    public function path(): Path
    {
        return $this->path;
    }

    public function read(int $length, int $offset): string
    {
        $offset = $offset < 0 ? ($this->size() ?? 0) + $offset : $offset;

        return $this->blobService
            ->getBlob($this->path->path(), (new GetBlobOptions())->withRange(new Range($offset, $offset + $length - 1)))
            ->content();
    }

    public function readLines(string $separator = "\n", ?int $length = null): Generator
    {
        $offset = 0;
        $content = '';
        $size = $this->size() ?? 0;

        while ($offset < $size) {
            $chunk = $this->read($length ?? (1024 * 1024 * 9), $offset);

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
        if ($this->blobProperties === null) {
            $this->blobProperties = $this->blobService->getBlobProperties($this->path->path());
        }

        return $this->blobProperties?->size();
    }
}
