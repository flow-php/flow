<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use Flow\Types\Type\Native\UnionType;

final readonly class UnionTypeNormalizer
{
    /**
     * @param UnionType<mixed, mixed> $type
     *
     * @return UnionType<mixed, mixed>
     */
    public function normalize(UnionType $type): UnionType
    {
        return (new TypeProjection())->union($type);
    }
}
