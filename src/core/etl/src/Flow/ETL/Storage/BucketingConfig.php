<?php

declare(strict_types=1);

namespace Flow\ETL\Storage;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Memory\Unit;
use SensitiveParameter;

use function Flow\Types\DSL\type_integer;
use function max;

final readonly class BucketingConfig implements Describable
{
    /**
     * @param int<1, max> $bucketsCount
     * @param int<1, max> $batchSize
     */
    public function __construct(
        public RowsStorage $storage,
        public int $bucketsCount,
        public int $batchSize,
        public Unit $memoryLimit,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['storage'] ?? null, RowsStorage::class),
            max(1, type_integer()->assert($data['buckets_count'] ?? null)),
            max(1, type_integer()->assert($data['batch_size'] ?? null)),
            Unit::fromBytes(type_integer()->assert($data['memory_limit'] ?? null)),
        );
    }

    /**
     * The storage behind the memory limit - a resident storage already holds everything in memory.
     */
    public function spilling(Backend $backend): RowsStorage
    {
        return $this->storage instanceof ResidentRowsStorage
            ? $this->storage
            : new SpillingRowsStorage($this->storage, $this->memoryLimit, $backend, new BatchSize($this->batchSize));
    }

    /**
     * null when the storage does not describe (a PSR cache, a user storage).
     */
    public function describe(): ?Description
    {
        return (
            $this->storage instanceof Describable
                ? Description::compose(self::class, [
                    'storage' => $this->storage,
                    'buckets_count' => $this->bucketsCount,
                    'batch_size' => $this->batchSize,
                    'memory_limit' => $this->memoryLimit->inBytes(),
                ])
                : null
        );
    }
}
