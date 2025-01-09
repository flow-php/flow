<?php

declare(strict_types=1);

namespace Flow\Tools\Rector;

use PhpParser\Node;
use PhpParser\Node\{Identifier, NullableType, UnionType};
use PHPStan\Type\{ArrayType, MixedType, Type, TypeCombinator};

trait ArrayDocBlockType
{
    private function isArrayType(Node $type) : bool
    {
        if ($type instanceof NullableType) {

            return $this->isArrayType($type->type);
        }

        if ($type instanceof UnionType) {
            foreach ($type->types as $subType) {
                if ($subType instanceof Identifier && $subType->toString() === 'array') {
                    return true;
                }
            }
        }

        if ($type instanceof Identifier) {
            return $type->toString() === 'array';
        }

        return false;
    }

    private function resolveType(Node $type) : Type
    {
        if ($type instanceof NullableType) {
            $innerType = $this->resolveType($type->type);

            return TypeCombinator::addNull($innerType);
        }

        if ($type instanceof UnionType) {
            $types = [];

            foreach ($type->types as $subType) {
                if ($subType instanceof Identifier && $subType->toString() === 'array') {
                    $types[] = new ArrayType(new MixedType(), new MixedType());
                } else {
                    $types[] = $this->nodeTypeResolver->getType($subType);
                }
            }

            return TypeCombinator::union(...$types);
        }

        return new ArrayType(new MixedType(), new MixedType());
    }
}
