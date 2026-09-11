<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use AsyncAws\S3\S3Client;
use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3SourceStream\Range;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;

use function array_pop;
use function explode;
use function ltrim;
use function str_contains;
use function strlen;

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
        $response = $this->s3Client->getObject([
            'Bucket' => $this->bucket,
            'Key' => ltrim($this->path->path(), '/'),
            'Range' => (new Range($offset, $length))->toString(),
        ]);

        return $response->getBody()->getContentAsString();
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
        if ($this->size === null) {
            $this->size = $this->s3Client->headObject([
                'Bucket' => $this->bucket,
                'Key' => ltrim($this->path->path(), '/'),
            ])->getContentLength();
        }

        return $this->size;
    }
}
