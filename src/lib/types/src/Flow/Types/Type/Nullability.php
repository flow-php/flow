<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use Flow\Types\Type;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\UnionType;

use function count;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_union;

final readonly class Nullability
{
    /**
     * Nullability has two spellings: OptionalType, and a UnionType containing NullType.
     * This class is the one place that knows both.
     *
     * @param Type<mixed> $type
     */
    public function is(Type $type): bool
    {
        if ($type instanceof OptionalType) {
            return true;
        }

        if ($type instanceof UnionType) {
            foreach ($type->types()->all() as $nextType) {
                if ($nextType instanceof NullType) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Strips exactly one level of nullability. Total - never throws, never null.
     * Unlike Types::reduceOptionals()'s first(), a multi-member nullable union stays a union
     * of its non-null members, so the strip is lossless.
     * Invariant: is(bare($type)) === false, for every $type.
     *
     * @param Type<mixed> $type
     *
     * @return Type<mixed>
     */
    public function bare(Type $type): Type
    {
        if ($type instanceof OptionalType) {
            return $type->base();
        }

        if ($type instanceof UnionType && $this->is($type)) {
            $members = $type->types()->without(type_null())->all();

            if (count($members) === 0) {
                return type_null();
            }

            if (count($members) === 1) {
                return $members[0];
            }

            return type_union($members[0], $members[1], ...array_slice($members, 2));
        }

        return $type;
    }

    /**
     * $result, made nullable iff ANY operand is
     *
     * @param Type<mixed> $result
     * @param Type<mixed> ...$operands
     *
     * @return Type<mixed>
     */
    public function any(Type $result, Type ...$operands): Type
    {
        foreach ($operands as $operand) {
            if ($this->is($operand)) {
                return type_optional($result);
            }
        }

        return $result;
    }

    /**
     * $result, made nullable iff EVERY operand is AND there is at least one
     *
     * @param Type<mixed> $result
     * @param Type<mixed> ...$operands
     *
     * @return Type<mixed>
     */
    public function all(Type $result, Type ...$operands): Type
    {
        if (count($operands) === 0) {
            return $result;
        }

        foreach ($operands as $operand) {
            if (!$this->is($operand)) {
                return $result;
            }
        }

        return type_optional($result);
    }
}
