<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use Flow\ETL\Schema\Definition;

use function in_array;

final readonly class CommonType
{
    private const CONTAINERS = [
        JsonDefinition::class,
        ListDefinition::class,
        MapDefinition::class,
        StructureDefinition::class,
    ];

    /**
     * @param Definition<mixed> $left
     * @param Definition<mixed> $right
     *
     * @return Definition<mixed>
     */
    public function merge(Definition $left, Definition $right): Definition
    {
        $nullable = $left->isNullable() || $right->isNullable();
        $metadata = $left->metadata()->merge($right->metadata());

        if (in_array($left::class, self::CONTAINERS, true) && in_array($right::class, self::CONTAINERS, true)) {
            return new JsonDefinition($left->entry(), $nullable, $metadata);
        }

        return new StringDefinition($left->entry(), $nullable, $metadata);
    }
}
