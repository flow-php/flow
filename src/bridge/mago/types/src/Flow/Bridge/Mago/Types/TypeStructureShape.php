<?php

declare(strict_types=1);

namespace Flow\Bridge\Mago\Types;

use Mago\Sdk\Analyzer\Invocation;
use Mago\Sdk\Analyzer\Type;
use Mago\Sdk\Analyzer\Type\ArrayItem;
use Mago\Sdk\Analyzer\Type\KeyedArrayType;
use Mago\Sdk\Analyzer\Type\NamedObjectType;
use Mago\Sdk\Analyzer\Type\ScalarType;
use Mago\Sdk\Analyzer\Type\ScalarTypeKind;

/**
 * Derives the array shape a `Flow\Types\DSL\type_structure()` call represents. Beyond Mago's
 * built-in `flow-php` plugin it understands `structure_element()` marker values: the member type
 * comes from the element's first type parameter and a literal `optional: true` second parameter
 * (TOptional) makes the shape key possibly undefined.
 */
final readonly class TypeStructureShape
{
    private const string STRUCTURE_TYPE = 'Flow\Types\Type\Logical\StructureType';

    public function __construct(
        private ElementShape $elementShape = new ElementShape(),
    ) {}

    public function derive(Invocation $invocation): ?Type
    {
        $elementsArray = $this->sealedKeyedArray($invocation->getArgument(0, 'elements')?->type);

        if ($elementsArray === null || $elementsArray->knownItems === null) {
            return null;
        }

        $allowExtra = false;

        if (($allowExtraArgument = $invocation->getArgument(1, 'allow_extra')) !== null) {
            $allowExtra = $allowExtraArgument->type?->getLiteralBool();

            if ($allowExtra === null) {
                return null;
            }
        }

        $items = [];

        foreach ($elementsArray->knownItems as $item) {
            $converted = $this->elementShape->derive($item);

            if ($converted === null) {
                return null;
            }

            $items[$this->itemKey($item)] = $converted;
        }

        $shape = Type::fromAtomic(new KeyedArrayType(
            knownItems: array_values($items),
            keyType: $allowExtra ? Type::fromAtomic(new ScalarType(ScalarTypeKind::ArrayKey)) : null,
            valueType: $allowExtra ? Type::mixed() : null,
            nonEmpty: $items !== [],
        ));

        return Type::fromAtomic(new NamedObjectType(
            name: self::STRUCTURE_TYPE,
            parameters: [$shape],
            variances: null,
            static: false,
            isThis: false,
            intersections: null,
            remappedParameters: false,
        ));
    }

    private function sealedKeyedArray(?Type $type): ?KeyedArrayType
    {
        if ($type === null || count($type->atomicTypes) !== 1) {
            return null;
        }

        $atomic = $type->atomicTypes[0];

        if (!$atomic instanceof KeyedArrayType || $atomic->keyType !== null || $atomic->valueType !== null) {
            return null;
        }

        return $atomic;
    }

    private function itemKey(ArrayItem $item): string
    {
        return $item->key->kind->name . ':' . ($item->key->value ?? $item->key->constant ?? '');
    }
}
