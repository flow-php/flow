<?php

declare(strict_types=1);

namespace Flow\Bridge\Mago\Types;

use Mago\Sdk\Analyzer\Type;
use Mago\Sdk\Analyzer\Type\ArrayItem;
use Mago\Sdk\Analyzer\Type\DerivedType;
use Mago\Sdk\Analyzer\Type\DerivedTypeKind;
use Mago\Sdk\Analyzer\Type\NamedObjectType;

final readonly class ElementShape
{
    private const string STRUCTURE_ELEMENT = 'Flow\Types\Type\Logical\StructureElement';

    private const string TYPE = 'Flow\Types\Type';

    public function derive(ArrayItem $item): ?ArrayItem
    {
        $namedObject = $this->singleNamedObject($item->type);

        if ($namedObject === null) {
            return null;
        }

        // Leaf types like StringType have no type parameters; Mago resolves T via @implements Type<X>.
        $member = strcasecmp($namedObject->name, self::STRUCTURE_ELEMENT) === 0
            ? $namedObject->parameters[0] ?? null
            : Type::fromAtomic(new DerivedType(DerivedTypeKind::TemplateType, [
                $item->type,
                Type::namedObject(self::TYPE),
                Type::literalString('T'),
            ]));

        if ($member === null) {
            return null;
        }

        $optional = $item->optional || $member->flags->possiblyUndefined;

        if (
            strcasecmp($namedObject->name, self::STRUCTURE_ELEMENT) === 0
            && ($namedObject->parameters[1] ?? null)?->getLiteralBool() === true
        ) {
            $optional = true;
        }

        return new ArrayItem($item->key, $optional, $member);
    }

    private function singleNamedObject(Type $type): ?NamedObjectType
    {
        if (count($type->atomicTypes) !== 1) {
            return null;
        }

        $atomic = $type->atomicTypes[0];

        return $atomic instanceof NamedObjectType ? $atomic : null;
    }
}
