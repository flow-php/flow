<?php

declare(strict_types=1);

namespace Flow\Bridge\Mago\Types\Tests\Mother;

use Mago\Sdk\Analyzer\Argument;
use Mago\Sdk\Analyzer\Invocation;
use Mago\Sdk\Analyzer\InvocationKind;
use Mago\Sdk\Analyzer\Type;
use Mago\Sdk\Analyzer\Type\ArrayItem;
use Mago\Sdk\Analyzer\Type\ArrayKey;
use Mago\Sdk\Analyzer\Type\ArrayKeyKind;
use Mago\Sdk\Analyzer\Type\KeyedArrayType;
use Mago\Sdk\Analyzer\Type\NamedObjectType;
use Mago\Sdk\Span;

final class InvocationMother
{
    public static function typeStructure(Type ...$argumentTypes): Invocation
    {
        $arguments = [];

        foreach ($argumentTypes as $type) {
            $arguments[] = new Argument(null, false, false, new Span(0, 1), 'probe', $type);
        }

        return new Invocation(
            InvocationKind::Function,
            'Flow\Types\DSL\type_structure',
            null,
            null,
            new Span(0, 1),
            $arguments,
        );
    }

    public static function sealedMap(ArrayItem ...$items): Type
    {
        return Type::fromAtomic(new KeyedArrayType(
            knownItems: array_values($items),
            keyType: null,
            valueType: null,
            nonEmpty: $items !== [],
        ));
    }

    public static function item(string $key, Type $valueType): ArrayItem
    {
        return new ArrayItem(new ArrayKey(ArrayKeyKind::String, $key), false, $valueType);
    }

    public static function flowType(Type $member): Type
    {
        return Type::fromAtomic(new NamedObjectType(
            name: 'Flow\Types\Type\Native\IntegerType',
            parameters: [$member],
            variances: null,
            static: false,
            isThis: false,
            intersections: null,
            remappedParameters: false,
        ));
    }

    public static function marker(Type $member, ?Type $optionalFlag): Type
    {
        return Type::fromAtomic(new NamedObjectType(
            name: 'Flow\Types\Type\Logical\StructureElement',
            parameters: $optionalFlag === null ? [$member] : [$member, $optionalFlag],
            variances: null,
            static: false,
            isThis: false,
            intersections: null,
            remappedParameters: false,
        ));
    }
}
