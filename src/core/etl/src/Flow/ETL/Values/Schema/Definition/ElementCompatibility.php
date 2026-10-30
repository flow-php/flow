<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Schema\Definition;

use Flow\ETL\Values\Schema\Definitions;
use Flow\Types\Type;
use Flow\Types\Type\Logical\OptionalType;

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

        $declaredDefinition = Definitions::of(
            $ref,
            $declaredNullable ? $declared->base() : $declared,
            $declaredNullable,
        );
        $givenDefinition = Definitions::of($ref, $givenNullable ? $given->base() : $given, $givenNullable);

        // A nested null value infers NullType, which no typed definition accepts on its own.
        if ($givenDefinition instanceof NullDefinition && $declaredNullable) {
            return true;
        }

        return $declaredDefinition->isCompatible($givenDefinition);
    }
}
