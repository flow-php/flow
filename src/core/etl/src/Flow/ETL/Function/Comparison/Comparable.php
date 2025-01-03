<?php

declare(strict_types=1);

namespace Flow\ETL\Function\Comparison;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\TypeDetector;

trait Comparable
{
    public function assertAllComparable(array $values, string $symbol) : void
    {
        $detector = new TypeDetector();

        $types = [];

        foreach ($values as $value) {
            $type = $detector->detectType($value);

            if (!in_array($type, $types, true)) {
                $types[] = $type;
            }
        }

        if (count($types) > 1) {
            foreach ($types as $nextType) {
                foreach ($types as $baseType) {
                    if (!$baseType->isComparableWith($nextType)) {
                        throw new InvalidArgumentException(\sprintf("Can't compare '(%s %s %s)' due to data type mismatch.", $baseType->toString(), $symbol, $nextType->toString()));
                    }
                }
            }
        }
    }

    public function assertComparable(mixed $left, mixed $right, string $symbol) : void
    {
        $detector = new TypeDetector();
        $baseType = $detector->detectType($left);
        $nextType = $detector->detectType($right);

        if (!$baseType->isComparableWith($nextType)) {
            throw new InvalidArgumentException(\sprintf("Can't compare '(%s %s %s)' due to data type mismatch.", $baseType->toString(), $symbol, $nextType->toString()));
        }
    }
}
