<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definitions;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;

final readonly class OnEachElementSchema
{
    /**
     * @param Type<mixed> $arrayType
     */
    public function of(Type $arrayType): Schema
    {
        return new Schema(Definitions::of('element', match (true) {
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
