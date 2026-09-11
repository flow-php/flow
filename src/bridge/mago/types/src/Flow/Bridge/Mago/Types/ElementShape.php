<?php

declare(strict_types=1);

namespace Flow\Bridge\Mago\Types;

use Mago\Sdk\Analyzer\Type;
use Mago\Sdk\Analyzer\Type\ArrayItem;
use Mago\Sdk\Analyzer\Type\NamedObjectType;

/**
 * Derives a single shape entry from a `type_structure()` elements-array item. The member type is
 * the value's first type parameter; a `structure_element()` marker whose second parameter
 * (TOptional) is a literal `true` makes the key possibly undefined.
 */
final readonly class ElementShape
{
    private const string STRUCTURE_ELEMENT = 'Flow\Types\Type\Logical\StructureElement';

    public function derive(ArrayItem $item): ?ArrayItem
    {
        $namedObject = $this->singleNamedObject($item->type);

        if ($namedObject === null) {
            return null;
        }

        $member = $namedObject->parameters[0] ?? null;

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
