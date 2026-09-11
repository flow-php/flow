<?php

declare(strict_types=1);

namespace Flow\Bridge\Mago\Types\Tests\Context;

use Mago\Sdk\Analyzer\Type;
use Mago\Sdk\Analyzer\Type\KeyedArrayType;
use Mago\Sdk\Analyzer\Type\NamedObjectType;
use PHPUnit\Framework\Assert;

final class DerivedShapeContext
{
    public static function keyedArray(?Type $derived): KeyedArrayType
    {
        Assert::assertNotNull($derived);

        $structure = $derived->atomicTypes[0];
        Assert::assertInstanceOf(NamedObjectType::class, $structure);
        Assert::assertSame('Flow\Types\Type\Logical\StructureType', $structure->name);

        $keyedArray = $structure->parameters[0]->atomicTypes[0] ?? null;
        Assert::assertInstanceOf(KeyedArrayType::class, $keyedArray);

        return $keyedArray;
    }

    /**
     * @return array<string, array{bool, Type}> key => [optional, member type instance]
     */
    public static function shape(?Type $derived): array
    {
        $shape = [];

        foreach (self::keyedArray($derived)->knownItems ?? [] as $item) {
            $shape[(string) $item->key->value] = [$item->optional, $item->type];
        }

        return $shape;
    }
}
