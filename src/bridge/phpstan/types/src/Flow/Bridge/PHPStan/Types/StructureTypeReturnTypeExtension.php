<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPStan\Types;

use Flow\Types\Type as FlowType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use PhpParser\Node\Expr\FuncCall;
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

        $optionalArrays = [];

        if (isset($args[1])) {
            $optionalArg = $scope->getType($args[1]->value);
            $optionalArrays = $optionalArg->getConstantArrays();
        }

        $results = [];

        foreach ($requiredArrays as $requiredArray) {
            // If we have optional arrays, combine them with each required array
            if (!empty($optionalArrays)) {
                foreach ($optionalArrays as $optionalArray) {
                    $results[] = $this->createResult($requiredArray, $optionalArray);
                }
            } else {
                $results[] = $this->createResult($requiredArray);
            }
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

    private function createResult(
        ConstantArrayType $requiredArrayType,
        ?ConstantArrayType $optionalArrayType = null,
    ): Type {
        $builder = ConstantArrayTypeBuilder::createEmpty();

        // Process required elements
        foreach ($requiredArrayType->getKeyTypes() as $key) {
            $valueType = $requiredArrayType->getOffsetValueType($key);
            [$type, $optional] = $this->extractOptional($valueType->getTemplateType(FlowType::class, 'T'));

            $builder->setOffsetValueType($key, $type, $optional);
        }

        // Process optional elements if provided
        if ($optionalArrayType !== null) {
            foreach ($optionalArrayType->getKeyTypes() as $key) {
                $valueType = $optionalArrayType->getOffsetValueType($key);
                [$type, $_wasOptional] = $this->extractOptional($valueType->getTemplateType(FlowType::class, 'T'));

                // Optional elements are always optional in the result structure
                $builder->setOffsetValueType($key, $type, true);
            }
        }

        return $builder->getArray();
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
