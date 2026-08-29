<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Unifier\NullabilityRule;
use Flow\Types\Type\Unifier\PromotingUnifier;

use function array_values;

final readonly class StructureValues
{
    /**
     * A structure's value type is the unification of its field types (Spark findWiderCommonType).
     *
     * @return Type<mixed>
     */
    public static function type(string $function, StructureType $structure): Type
    {
        return (
            (new PromotingUnifier())->unifyAll(
                NullabilityRule::ANY,
                ...array_values($structure->elements() + $structure->optionalElements()),
            ) ?? throw SchemaNotDerivableException::function(
                $function,
                'the structure operand declares "' . $structure->toString() . '", whose field types do not unify',
            )
        );
    }
}
