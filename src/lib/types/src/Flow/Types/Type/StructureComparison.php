<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use Flow\Types\Type\Logical\StructureType;

use function count;

final readonly class StructureComparison
{
    /**
     * Identity - positional, and the optional flag is part of it.
     *
     * @param StructureType<array<array-key, mixed>> $left
     * @param StructureType<array<array-key, mixed>> $right
     */
    public function identical(StructureType $left, StructureType $right, Comparator $comparator): bool
    {
        if ($left->allowsExtra() !== $right->allowsExtra()) {
            return false;
        }

        if (count($left->elements()) !== count($right->elements())) {
            return false;
        }

        foreach ($left->elements() as $position => $element) {
            $other = $right->elements()[$position];

            if ($element->name !== $other->name) {
                return false;
            }

            if ($element->optional !== $other->optional) {
                return false;
            }

            if (!$comparator->equals($element->type, $other->type)) {
                return false;
            }
        }

        return true;
    }
}
