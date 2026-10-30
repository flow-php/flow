<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\BlockFactory;

use function Flow\Filesystem\DSL\path;
use function is_bool;
use function is_int;
use function is_string;

/**
 * @type S3Options = array{block_location: string, file_fast_path: bool, part_size: int}
 */
final class Options
{
    private BlockFactory $blockFactory;

    private bool $fileFastPath = true;

    /** @var int<1, max> */
    private int $partSize = 1024 * 1024 * 5;

    private readonly Path $tmpDir;

    public function __construct()
    {
        $this->blockFactory = new NativeLocalFileBlocksFactory();
        $this->tmpDir = path('aws-s3://_$azure_flow_tmp$/');
    }

    public function blockFactory(): BlockFactory
    {
        return $this->blockFactory;
    }

    public function fileFastPath(): bool
    {
        return $this->fileFastPath;
    }

    /**
     * @return int<1, max>
     */
    public function partSize(): int
    {
        return $this->partSize;
    }

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     *
     * @throws InvalidArgumentException
     */
    public static function fromArray(array $data): self
    {
        if (
            !is_string($data['block_location'] ?? null)
            || !is_bool($data['file_fast_path'] ?? null)
            || !is_int($data['part_size'] ?? null)
        ) {
            throw new InvalidArgumentException(
                'AsyncAWS options must hold block_location (string), file_fast_path (bool) and part_size (int)',
            );
        }

        return (new self())
            ->withBlockFactory(new NativeLocalFileBlocksFactory($data['block_location']))
            ->withFileFastPath($data['file_fast_path'])
            ->withBlockSize($data['part_size']);
    }

    /**
     * @return null|S3Options null when the block factory is not the native local one
     */
    public function normalize(): ?array
    {
        if (!$this->blockFactory instanceof NativeLocalFileBlocksFactory) {
            return null;
        }

        return [
            'block_location' => $this->blockFactory->blockLocation(),
            'file_fast_path' => $this->fileFastPath,
            'part_size' => $this->partSize,
        ];
    }

    public function tmpDir(): Path
    {
        return $this->tmpDir;
    }

    public function withBlockFactory(BlockFactory $blockFactory): self
    {
        $this->blockFactory = $blockFactory;

        return $this;
    }

    public function withBlockSize(int $bytes): self
    {
        if ($bytes < (1024 * 1024 * 5)) {
            throw new InvalidArgumentException('Block size must be at least 5Mb');
        }

        $this->partSize = $bytes;

        return $this;
    }

    /**
     * When enabled (default), list() on a single-file path will first attempt a HEAD on the object.
     * If the object exists, list() yields just that single FileStatus and never issues listObjectsV2.
     * This avoids the s3:ListBucket permission requirement and an extra round-trip when reading single files.
     * Disable if your workloads typically pass folder/prefix paths to list(), to skip the (failing) HEAD.
     */
    public function withFileFastPath(bool $enabled = true): self
    {
        $this->fileFastPath = $enabled;

        return $this;
    }
}
