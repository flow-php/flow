<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPStan\Types;

use Flow\Types\Type as FlowType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureElement;
use Flow\Types\Type\Logical\StructureType;
use PhpParser\Node\Expr;
use PhpParser\Node\Expr\Array_;
use PhpParser\Node\Expr\ConstFetch;
use PhpParser\Node\Expr\FuncCall;
use PhpParser\Node\Scalar\Int_;
use PhpParser\Node\Scalar\String_;
use PHPStan\Analyser\Scope;
use PHPStan\Reflection\FunctionReflection;
use PHPStan\Type\Constant\ConstantArrayType;
use PHPStan\Type\Constant\ConstantArrayTypeBuilder;
use PHPStan\Type\DynamicFunctionReturnTypeExtension;
use PHPStan\Type\ErrorType;
use PHPStan\Type\Generic\GenericObjectType;
use PHPStan\Type\IntersectionType;
use PHPStan\Type\ObjectType;
use PHPStan\Type\Type;
use PHPStan\Type\TypeCombinator;

final class StructureTypeReturnTypeExtension implements DynamicFunctionReturnTypeExtension
{
    public function getTypeFromFunctionCall(
        FunctionReflection $functionReflection,
        FuncCall $functionCall,
        Scope $scope,
    ): ?Type {
        $args = $functionCall->getArgs();

        if (!isset($args[0])) {
            return null;
        }

        $requiredArg = $scope->getType($args[0]->value);
        $requiredArrays = $requiredArg->getConstantArrays();

        if (count($requiredArrays) === 0) {
            return null;
        }

        $markerFlags = $this->markerOptionalFlags($args[0]->value);

        $results = [];

        foreach ($requiredArrays as $requiredArray) {
            $results[] = $this->createResult($requiredArray, $markerFlags);
        }

        $arrayShapeType = TypeCombinator::union(...$results);

        return new IntersectionType([
            new ObjectType(StructureType::class),
            new GenericObjectType(FlowType::class, [$arrayShapeType]),
        ]);
    }

    public function isFunctionSupported(FunctionReflection $functionReflection): bool
    {
        return $functionReflection->getName() === 'Flow\Types\DSL\type_structure';
    }

    /**
     * @param array<array-key, bool> $markerFlags
     */
    private function createResult(ConstantArrayType $requiredArrayType, array $markerFlags): Type
    {
        $builder = ConstantArrayTypeBuilder::createEmpty();

        // Process required elements
        foreach ($requiredArrayType->getKeyTypes() as $key) {
            $valueType = $requiredArrayType->getOffsetValueType($key);

            // a structure_element() marker value: the member type is the element's template, the
            // optional flag comes from the call expression (the type alone cannot carry it)
            if ((new ObjectType(StructureElement::class))->isSuperTypeOf($valueType)->yes()) {
                $builder->setOffsetValueType(
                    $key,
                    $valueType->getTemplateType(StructureElement::class, 'T'),
                    $markerFlags[$key->getValue()] ?? true,
                );

                continue;
            }

            [$type, $optional] = $this->extractOptional($valueType->getTemplateType(FlowType::class, 'T'));

            $builder->setOffsetValueType($key, $type, $optional);
        }

        return $builder->getArray();
    }

    /**
     * Reads the literal `optional:` flag off each structure_element() call in the array literal.
     * A marker whose flag cannot be read statically is treated as optional - under-promising
     * presence is the safe direction for an array shape.
     *
     * @return array<array-key, bool>
     */
    private function markerOptionalFlags(Expr $elements): array
    {
        if (!$elements instanceof Array_) {
            return [];
        }

        $flags = [];

        foreach ($elements->items as $item) {
            if ($item->key === null || !$item->value instanceof FuncCall) {
                continue;
            }

            $key = match (true) {
                $item->key instanceof String_ => $item->key->value,
                $item->key instanceof Int_ => $item->key->value,
                default => null,
            };

            if ($key === null) {
                continue;
            }

            $optional = false;

            foreach ($item->value->getArgs() as $position => $arg) {
                $isOptionalArg = $arg->name?->toString() === 'optional' || $arg->name === null && $position === 2;

                if ($isOptionalArg) {
                    $optional = !($arg->value instanceof ConstFetch && $arg->value->name->toLowerString() === 'false');
                }
            }

            $flags[$key] = $optional;
        }

        return $flags;
    }

    /**
     * @return array{Type, bool}
     */
    private function extractOptional(Type $type): array
    {
        $optionalType = $type->getTemplateType(OptionalType::class, 'T');

        if ($optionalType instanceof ErrorType) {
            return [$type, false];
        }

        return [$optionalType, true];
    }
}
