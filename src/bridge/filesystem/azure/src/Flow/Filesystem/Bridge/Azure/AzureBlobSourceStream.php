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

use function count;
use function explode;
use function str_contains;
use function strlen;
use function strpos;
use function substr;
use function substr_count;

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
            // Read a chunk of the file
            $chunk = $this->read($length ?? (1024 * 1024 * 9), $offset);
            $offset += strlen($chunk);
            $content .= $chunk;

            // no separators found in the chunk, we are still processing single line
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
            } elseif (substr_count($content, $separator) === 1) {
                $pos = strpos($content, $separator);

                if ($pos !== false) {
                    yield substr($content, 0, $pos);
                    $content = substr($content, $pos + 1);
                }
            }
        }

        // Yield the remaining content if it's not empty
        if ($content) {
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
