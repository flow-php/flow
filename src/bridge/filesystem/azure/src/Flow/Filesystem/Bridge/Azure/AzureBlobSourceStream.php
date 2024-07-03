<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure;

use Flow\Azure\SDK\BlobService\GetBlob\GetBlobOptions;
use Flow\Azure\SDK\BlobService\GetBlobProperties\BlobProperties;
use Flow\Azure\SDK\{BlobService, BlobServiceInterface};
use Flow\Filesystem\{Path, SourceStream};

final class AzureBlobSourceStream implements SourceStream
{
    private ?BlobProperties $blobProperties = null;

    public function __construct(private readonly Path $path, private readonly BlobServiceInterface $blobService)
    {
    }

    public function close() : void
    {
        // do nothing as we can't close Azure Blob since we are just reading parts of it at once
    }

    public function content() : string
    {
        return $this->blobService->getBlob($this->path->path())->content();
    }

    public function isOpen() : bool
    {
        return true;
    }

    public function iterate(int $length = 1) : \Generator
    {
        $offset = 0;

        while ($offset < $this->size()) {
            yield $this->read($length, $offset);
            $offset += $length;
        }
    }

    public function path() : Path
    {
        return $this->path;
    }

    public function read(int $length, int $offset) : string
    {
        $offset = $offset < 0 ? $this->size() + $offset : $offset;

        return $this->blobService->getBlob(
            $this->path->path(),
            (new GetBlobOptions())->withRange(new BlobService\GetBlob\Range($offset, $offset + $length - 1))
        )->content();
    }

    /**
     * @psalm-suppress PossiblyFalseArgument
     * @psalm-suppress PossiblyFalseOperand
     */
    public function readLines(string $separator = "\n", ?int $length = null) : \Generator
    {
        $offset = 0;
        $content = '';

        while ($offset < $this->size()) {
            // Read a chunk of the file
            $chunk = $this->read($length ?? 1024 * 1024 * 9, $offset);
            $offset += \strlen($chunk);
            $content .= $chunk;

            // no separators found in the chunk, we are still processing single line
            if (!\str_contains($content, $separator)) {
                continue;
            }

            if (\substr_count($content, $separator) > 1) {
                /**
                 * @phpstan-ignore-next-line
                 */
                $lines = \array_filter(\explode($separator, $content));

                // Yield all lines except the last one
                foreach (\array_slice($lines, 0, -1) as $line) {
                    yield $line;
                }

                // The last line is incomplete, so we need to keep it for the next iteration
                $content = \end($lines);
            } elseif (\substr_count($content, $separator) === 1) {
                // Split the content by the separator
                /**
                 * @phpstan-ignore-next-line
                 */
                yield \substr($content, 0, \strpos($content, $separator));
                $content = \substr($content, \strpos($content, $separator) + 1);
            }
        }

        // Yield the remaining content if it's not empty
        if ($content) {
            yield $content;
        }
    }

    public function size() : ?int
    {
        if ($this->blobProperties === null) {
            $this->blobProperties = $this->blobService->getBlobProperties($this->path->path());
        }

        return $this->blobProperties?->size();
    }
}
