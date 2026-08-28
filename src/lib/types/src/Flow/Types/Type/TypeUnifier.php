<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use Flow\Types\Type;
use Flow\Types\Type\Unifier\NullabilityRule;

interface TypeUnifier
{
    /**
     * Pairwise. Called by ContainerUnification at depth >= 1, where nullability composes by OR.
     * Not the root entry point.
     *
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     *
     * @return null|Type<mixed> null = "no common type under this policy" - a value, not an exception
     */
    public function unify(Type $left, Type $right): ?Type;

    /**
     * N-ary, and the root entry point every caller uses. Strips every root's nullability,
     * folds the bases under this policy, then re-wraps by $rule read from the original operands.
     * $rule is first because PHP requires the variadic last.
     *
     * @param Type<mixed> ...$types
     *
     * @return null|Type<mixed>
     */
    public function unifyAll(NullabilityRule $rule, Type ...$types): ?Type;
}
