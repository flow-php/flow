<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Type\Comparison\Operator;

final readonly class ValueComparator
{
    public function __construct(private Comparator $comparator = new Comparator(), private TypeDetector $detector = new TypeDetector())
    {
    }

    public function assertAllComparable(array $values, Operator|string $operator) : void
    {
        $operator = \is_string($operator) ? Operator::from($operator) : $operator;

        $types = [];

        foreach ($values as $value) {
            $type = $this->detector->detectType($value);

            if (!in_array($type, $types, true)) {
                $types[] = $type;
            }
        }

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

    public function assertComparable(mixed $left, mixed $right, Operator|string $operator) : void
    {
        $operator = \is_string($operator) ? Operator::from($operator) : $operator;

        $baseType = $this->detector->detectType($left);
        $nextType = $this->detector->detectType($right);

        if (!$this->comparator->comparable($baseType, $nextType)) {
            throw new InvalidArgumentException(\sprintf("Can't compare '(%s %s %s)' due to data type mismatch.", $baseType->toString(), $operator->value, $nextType->toString()));
        }
    }
}
