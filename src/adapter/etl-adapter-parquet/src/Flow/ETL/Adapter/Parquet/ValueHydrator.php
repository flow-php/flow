<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Schema\Definition;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\UuidType;

final readonly class ValueHydrator
{
    public function __construct() {}

    /**
     * @param Definition<mixed> $definition
     */
    public function hydrate(mixed $value, Definition $definition): mixed
    {
        if ($value === null) {
            return null;
        }

        $type = $definition->type();

        if ($type instanceof OptionalType) {
            $type = $type->base();
        }

        if ($type instanceof JsonType || $type instanceof UuidType) {
            return $type->cast($value);
        }

        return $value;
    }
}
