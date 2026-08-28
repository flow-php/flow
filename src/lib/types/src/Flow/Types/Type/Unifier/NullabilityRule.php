<?php

declare(strict_types=1);

namespace Flow\Types\Type\Unifier;

use Flow\Types\Type;
use Flow\Types\Type\Nullability;

enum NullabilityRule
{
    case ANY;
    case ALL;

    /**
     * @param Type<mixed> $result
     * @param Type<mixed> ...$operands
     *
     * @return Type<mixed>
     */
    public function apply(Nullability $nullability, Type $result, Type ...$operands): Type
    {
        return match ($this) {
            self::ANY => $nullability->any($result, ...$operands),
            self::ALL => $nullability->all($result, ...$operands),
        };
    }
}
