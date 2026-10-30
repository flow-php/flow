<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Sort;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Storage\RowsStorage;
use SensitiveParameter;

final readonly class ExternalSortConfig implements Describable
{
    /**
     * @param BucketingConfig $bucketing - the spill side; its memory limit is what the sort holds before it spills
     *                                    sorted runs
     * @param null|RowsStorage $merge - null means the merged runs go to the same storage the spill uses
     */
    public function __construct(
        public BucketingConfig $bucketing,
        public ?RowsStorage $merge = null,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['bucketing'] ?? null, BucketingConfig::class),
            ($data['merge'] ?? null) === null ? null : $rebuild->of($data['merge'], RowsStorage::class),
        );
    }

    public function describe(): ?Description
    {
        if ($this->merge !== null && !$this->merge instanceof Describable) {
            return null;
        }

        return Description::compose(self::class, [
            'bucketing' => $this->bucketing,
            'merge' => $this->merge,
        ]);
    }
}
