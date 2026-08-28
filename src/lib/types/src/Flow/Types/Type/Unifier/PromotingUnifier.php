<?php

declare(strict_types=1);

namespace Flow\Types\Type\Unifier;

use Flow\Types\Type;
use Flow\Types\Type\ContainerUnification;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EmptyArrayType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\Nullability;
use Flow\Types\Type\TypeUnifier;

use function array_values;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;

final readonly class PromotingUnifier implements TypeUnifier
{
    public function __construct(
        private ContainerUnification $containers = new ContainerUnification(),
        private Nullability $nullability = new Nullability(),
    ) {}

    public function unify(Type $left, Type $right): ?Type
    {
        if (type_equals($left, $right)) {
            return $left;
        }

        if ($left instanceof NullType) {
            return $this->nullability->bare($right);
        }

        if ($right instanceof NullType) {
            return $this->nullability->bare($left);
        }

        $leftIsContainer = $left instanceof ListType || $left instanceof MapType || $left instanceof StructureType;
        $rightIsContainer = $right instanceof ListType || $right instanceof MapType || $right instanceof StructureType;

        if ($left instanceof OptionalType || $right instanceof OptionalType || $leftIsContainer && $rightIsContainer) {
            return $this->containers->unify($left, $right, $this->unify(...), (new StrictUnifier())->unify(...));
        }

        if (
            ($left instanceof IntegerType || $left instanceof FloatType)
            && ($right instanceof IntegerType || $right instanceof FloatType)
        ) {
            return type_float();
        }

        if (
            ($left instanceof DateType || $left instanceof DateTimeType)
            && ($right instanceof DateType || $right instanceof DateTimeType)
        ) {
            return type_datetime();
        }

        // TypeCoercion.scala:112 stringPromotion - any non-container atomic except boolean promotes to string.
        if ($left instanceof StringType xor $right instanceof StringType) {
            $other = $left instanceof StringType ? $right : $left;

            if (
                !$other instanceof BooleanType
                && !$other instanceof ArrayType
                && !$other instanceof EmptyArrayType
                && !$other instanceof JsonType
                && !$other instanceof ListType
                && !$other instanceof MapType
                && !$other instanceof StructureType
            ) {
                return type_string();
            }
        }

        return null;
    }

    public function unifyAll(NullabilityRule $rule, Type ...$types): ?Type
    {
        // TypeCoercion.scala:175-186 findWiderCommonType: the pairwise step does not satisfy the
        // associative law for StringType (or nested StringType in a list), so string-typed operands
        // are deduplicated and hoisted to the front before the fold.
        $hasStringType = static function (Type $type) use (&$hasStringType): bool {
            if ($type instanceof StringType) {
                return true;
            }

            return $type instanceof ListType && $hasStringType($type->element());
        };

        $stringTypes = [];
        $nonStringTypes = [];

        foreach ($types as $type) {
            $base = $this->nullability->bare($type);

            if ($hasStringType($base)) {
                $stringTypes[$base->toString()] = $base;
            } else {
                $nonStringTypes[] = $base;
            }
        }

        $result = type_null();

        foreach ([...array_values($stringTypes), ...$nonStringTypes] as $base) {
            $result = $this->unify($result, $base);

            if ($result === null) {
                return null;
            }
        }

        return $rule->apply($this->nullability, $result, ...$types);
    }
}
