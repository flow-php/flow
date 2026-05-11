<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use AsyncAws\S3\S3Client;
use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3SourceStream\Range;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;

final class AsyncAWSS3SourceStream implements SourceStream
{
    private ?int $size = null;

    public function __construct(
        private readonly Path $path,
        private readonly string $bucket,
        private readonly S3Client $s3Client,
    ) {}

    public function close(): void {}

    public function content(): string
    {
        return $this->s3Client
            ->getObject([
                'Bucket' => $this->bucket,
                'Key' => ltrim($this->path->path(), '/'),
            ])
            ->getBody()
            ->getContentAsString();
    }

    public function isOpen(): bool
    {
        return true;
    }

    public function iterate(int $length = 1): \Generator
    {
        for ($offset = 0; $offset < $this->size(); $offset += $length) {
            yield $this->read($length, $offset);
        }
    }

    public function path(): Path
    {
        return $this->path;
    }

    public function read(int $length, int $offset): string
    {
        $response = $this->s3Client->getObject([
            'Bucket' => $this->bucket,
            'Key' => ltrim($this->path->path(), '/'),
            'Range' => (new Range($offset, $length))->toString(),
        ]);

        return $response->getBody()->getContentAsString();
    }

    public function readLines(string $separator = "\n", ?int $length = null): \Generator
    {
        $offset = 0;
        $content = '';

        while ($offset < $this->size()) {
            // Read a chunk of the file
            $chunk = $this->read($length ?? (1024 * 1024 * 9), $offset);
            $offset += \strlen($chunk);
            $content .= $chunk;

            // no separators found in the chunk, we are still processing single line
            if (!\str_contains($content, $separator)) {
                continue;
            }

            if (\substr_count($content, $separator) > 1) {
                /** @phpstan-ignore argument.type */
                $lines = \explode($separator, $content);

                $lastIndex = \count($lines) - 1;

                for ($i = 0; $i < $lastIndex; $i++) {
                    yield $lines[$i];
                }

                $content = $lines[$lastIndex];
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

    public function size(): ?int
    {
        if ($this->size === null) {
            $this->size = $this->s3Client->headObject([
                'Bucket' => $this->bucket,
                'Key' => \ltrim($this->path->path(), '/'),
            ])->getContentLength();
        }

        return $this->size;
    }
}
