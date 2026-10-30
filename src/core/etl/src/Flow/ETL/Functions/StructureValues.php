<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureElement;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Unifier\NullabilityRule;
use Flow\Types\Type\Unifier\PromotingUnifier;

use function array_map;

final readonly class StructureValues
{
    /**
     * A structure's value type is the unification of its field types
     *
     * @return Type<mixed>
     */
    public static function type(string $function, StructureType $structure): Type
    {
        return (
            (new PromotingUnifier())->unifyAll(NullabilityRule::ANY, ...array_map(
                static fn(StructureElement $element): Type => $element->type,
                $structure->elements(),
            )) ?? throw SchemaNotDerivableException::function(
                $function,
                'the structure operand declares "' . $structure->toString() . '", whose field types do not unify',
            )
        );
    }
}
