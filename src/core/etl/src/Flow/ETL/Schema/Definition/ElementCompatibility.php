<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use Flow\Types\Type;
use Flow\Types\Type\Logical\OptionalType;

use function Flow\ETL\DSL\definition_from_type;

final readonly class ElementCompatibility
{
    /**
     * @param Type<mixed> $declared
     * @param Type<mixed> $given
     */
    public function isCompatible(string $ref, Type $declared, Type $given): bool
    {
        $declaredNullable = $declared instanceof OptionalType;
        $givenNullable = $given instanceof OptionalType;

        $declaredDefinition = definition_from_type(
            $ref,
            $declaredNullable ? $declared->base() : $declared,
            $declaredNullable,
        );
        $givenDefinition = definition_from_type($ref, $givenNullable ? $given->base() : $given, $givenNullable);

        // A nested null value infers NullType, which no typed definition accepts on its own.
        if ($givenDefinition instanceof NullDefinition && $declaredNullable) {
            return true;
        }

        return $declaredDefinition->isCompatible($givenDefinition);
    }
}
