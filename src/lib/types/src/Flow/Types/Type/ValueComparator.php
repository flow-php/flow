<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Comparison\Operator;

final readonly class ValueComparator
{
    public function __construct(private Comparator $comparator = new Comparator())
    {
    }

    /**
     * @param array<array-key, Type<mixed>> $types
     */
    public function assertAllTypesComparable(array $types, Operator|string $operator) : void
    {
        $operator = \is_string($operator) ? Operator::from($operator) : $operator;

        if (count($types) > 1) {
            foreach ($types as $nextType) {
                foreach ($types as $baseType) {
                    if (!$this->comparator->comparable($baseType, $nextType)) {
                        throw new InvalidArgumentException(\sprintf("Can't compare '(%s %s %s)' due to data type mismatch.", $baseType->toString(), $operator->value, $nextType->toString()));
                    }
                }
            }
        }
    }

    /**
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     */
    public function assertComparableTypes(Type $left, Type $right, Operator|string $operator) : void
    {
        $operator = \is_string($operator) ? Operator::from($operator) : $operator;

        if (!$this->comparator->comparable($left, $right)) {
            throw new InvalidArgumentException(\sprintf("Can't compare '(%s %s %s)' due to data type mismatch.", $left->toString(), $operator->value, $right->toString()));
        }
    }
}
