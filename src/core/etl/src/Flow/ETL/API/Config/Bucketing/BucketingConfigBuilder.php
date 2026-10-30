<?php

declare(strict_types=1);

namespace Flow\ETL\API\Config\Bucketing;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Planning\Logical\Node\BucketingAlgorithmBuilder;
use Flow\ETL\Runtime\MemoryLimit;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Memory\Unit;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

final class BucketingConfigBuilder implements BucketingAlgorithmBuilder
{
    /**
     * @var int<1, max>
     */
    public const int DEFAULT_BATCH_SIZE = 1000;

    /**
     * @var int<1, max>
     */
    private int $batchSize = self::DEFAULT_BATCH_SIZE;

    /**
     * @var int<1, max>
     */
    private int $bucketsCount;

    private ?Unit $memoryLimit = null;

    private ?RowsStorage $storage = null;

    /**
     * @param int<1, max> $bucketsCount
     */
    public function __construct(
        private readonly string $spillDirectory,
        int $bucketsCount,
    ) {
        $this->bucketsCount = $bucketsCount;
    }

    public static function groupBy(): self
    {
        return new self('/flow-php-group-by/', 64);
    }

    public static function repartition(): self
    {
        return new self('/flow-php-repartition/', 64);
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

    public function build(Path $spillRoot, Backend $backend): BucketingConfig
    {
        return new BucketingConfig(
            $this->storage ?? new FilesystemRowsStorage(
                new NativeLocalFilesystem(),
                $spillRoot->suffix($this->spillDirectory),
                $backend,
                $this->batchSize,
            ),
            $this->bucketsCount,
            $this->batchSize,
            $this->memoryLimit ?? MemoryLimit::default(),
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

    /**
     * The process memory past which rows move to the storage; defaults to MemoryLimit::default().
     */
    public function memoryLimit(Unit $memoryLimit): self
    {
        if ($memoryLimit->inBytes() < 1) {
            throw new InvalidArgumentException('Bucketing memory limit must be greater than 0 bytes');
        }

        $this->memoryLimit = $memoryLimit;

        return $this;
    }

    public function storage(RowsStorage $storage): self
    {
        $this->storage = $storage;

        return $this;
    }
}
