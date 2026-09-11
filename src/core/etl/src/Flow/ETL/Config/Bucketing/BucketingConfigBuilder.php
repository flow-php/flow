<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Bucketing;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Bucketing\Storage\FilesystemBuckets;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

final class BucketingConfigBuilder
{
    /**
     * @var int<1, max>
     */
    private int $batchSize = 1000;

    /**
     * @var int<1, max>
     */
    private int $bucketsCount;

    private ?BucketsStorage $storage = null;

    /**
     * @param int<1, max> $bucketsCount
     */
    public function __construct(
        private readonly string $spillDirectory,
        int $bucketsCount,
    ) {
        $this->bucketsCount = $bucketsCount;
    }

    /**
     * @param int<1, max> $batchSize
     */
    public function batchSize(int $batchSize): self
    {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be at least 1');
        }

        $this->batchSize = $batchSize;

        return $this;
    }

    public function build(Path $spillRoot): BucketingConfig
    {
        return new BucketingConfig(
            $this->storage ?? new FilesystemBuckets(
                new NativeLocalFilesystem(),
                $spillRoot->suffix($this->spillDirectory),
                $this->batchSize,
            ),
            $this->bucketsCount,
            $this->batchSize,
        );
    }

    /**
     * @param int<1, max> $bucketsCount
     */
    public function bucketsCount(int $bucketsCount): self
    {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($bucketsCount < 1) {
            throw new InvalidArgumentException('Buckets count must be greater than 0');
        }

        $this->bucketsCount = $bucketsCount;

        return $this;
    }

    public function storage(BucketsStorage $storage): self
    {
        $this->storage = $storage;

        return $this;
    }
}
