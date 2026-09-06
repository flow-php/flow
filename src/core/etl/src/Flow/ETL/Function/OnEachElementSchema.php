<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Schema;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\schema;

final readonly class OnEachElementSchema
{
    /**
     * @param Type<mixed> $arrayType
     */
    public function of(Type $arrayType): Schema
    {
        return schema(definition_from_type('element', match (true) {
            $arrayType instanceof ListType => $arrayType->element(),
            $arrayType instanceof MapType => $arrayType->value(),
            $arrayType instanceof StructureType => StructureValues::type('on_each', $arrayType),
            default => throw SchemaNotDerivableException::function(
                'on_each',
                'the array operand declares "' . $arrayType->toString() . '", which has no element type',
            ),
        }));
    }
}
