<?php

declare(strict_types=1);

namespace Flow\ETL\Function\Comparison;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\TypeDetector;

trait Comparable
{
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
