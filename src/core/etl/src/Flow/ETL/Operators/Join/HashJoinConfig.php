<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Join;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Values\Memory\Unit;
use SensitiveParameter;

use function Flow\Types\DSL\type_integer;

final readonly class HashJoinConfig implements Describable
{
    /**
     * @param Unit $broadcastLimit the right side's size under which a join read by workers broadcasts it
     */
    public function __construct(
        public BucketingConfig $bucketing,
        public Unit $broadcastLimit,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['bucketing'] ?? null, BucketingConfig::class),
            Unit::fromBytes(type_integer()->assert($data['broadcast_limit'] ?? null)),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'bucketing' => $this->bucketing,
            'broadcast_limit' => $this->broadcastLimit->inBytes(),
        ]);
    }
}
