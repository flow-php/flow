<?php

declare(strict_types=1);

namespace Flow\Types\Type\Unifier;

use Flow\Types\Type;
use Flow\Types\Type\ContainerUnification;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Nullability;
use Flow\Types\Type\TypeUnifier;

use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_null;

final readonly class StrictUnifier implements TypeUnifier
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
            return $this->containers->unify($left, $right, $this->unify(...), $this->unify(...));
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

        return null;
    }

    public function unifyAll(NullabilityRule $rule, Type ...$types): ?Type
    {
        // TypeCoercionHelper.scala:213-221: a plain fold - no partition, no distinct, no string hoist.
        $result = type_null();

        foreach ($types as $type) {
            $result = $this->unify($result, $this->nullability->bare($type));

            if ($result === null) {
                return null;
            }
        }

        return $rule->apply($this->nullability, $result, ...$types);
    }
}
