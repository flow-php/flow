<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Comparison;

use DateInterval;
use DateTimeImmutable;
use Flow\Types\Type;
use Flow\Types\Type\Logical\NumericStringType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;

use function Flow\Types\DSL\type_bare;
use function is_string;
use function strcmp;

/**
 * NaN is the largest value and equal to itself, as in the sort order.
 */
final readonly class ValueOrder
{
    /**
     * A float operand may hold NaN, which PHP's operators never order.
     */
    private bool $floats;

    private bool $numericStrings;

    /**
     * Every operand is an integer, float or boolean: PHP's own operators order them exactly as compare() does.
     */
    private bool $scalars;

    public function __construct(Type ...$operands)
    {
        $floats = false;
        $numericStrings = false;
        $scalars = $operands !== [];

        foreach ($operands as $operand) {
            $bare = type_bare($operand);
            $floats = $floats || $bare instanceof FloatType;
            $numericStrings = $numericStrings || $bare instanceof NumericStringType;
            $scalars =
                $scalars
                && ($bare instanceof IntegerType || $bare instanceof FloatType || $bare instanceof BooleanType);
        }

        $this->floats = $floats;
        $this->numericStrings = $numericStrings;
        $this->scalars = $scalars;
    }

    public function compare(mixed $left, mixed $right): int
    {
        if ($left !== $left) {
            return $right !== $right ? 0 : 1;
        }

        if ($right !== $right) {
            return -1;
        }

        if (!$this->numericStrings && is_string($left) && is_string($right)) {
            return strcmp($left, $right) <=> 0;
        }

        if ($left instanceof DateInterval && $right instanceof DateInterval) {
            $reference = new DateTimeImmutable('@0');

            return $reference->add($left) <=> $reference->add($right);
        }

        // @mago-expect analysis:mixed-operand(2)
        return $left <=> $right;
    }

    /**
     * $lefts[i] < $rights[i] for every i; null where either side is null.
     *
     * @param list<mixed> $lefts
     * @param list<mixed> $rights
     *
     * @return list<null|bool>
     */
    public function lessThan(array $lefts, array $rights): array
    {
        $results = [];

        if ($this->scalars && !$this->floats) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($lefts as $i => $left) {
                // @mago-ignore analysis:mixed-assignment
                $right = $rights[$i];
                // @mago-expect analysis:mixed-operand(2)
                $results[] = $left === null || $right === null ? null : $left < $right;
            }

            return $results;
        }

        if ($this->scalars) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($lefts as $i => $left) {
                // @mago-ignore analysis:mixed-assignment
                $right = $rights[$i];
                // @mago-expect analysis:mixed-operand(2)
                $results[] =
                    $left === null || $right === null
                        ? null
                        : ($right !== $right ? $left === $left : $left === $left && $left < $right);
            }

            return $results;
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($lefts as $i => $left) {
            // @mago-ignore analysis:mixed-assignment
            $right = $rights[$i];
            $results[] = $left === null || $right === null ? null : $this->compare($left, $right) < 0;
        }

        return $results;
    }

    /**
     * $lefts[i] <= $rights[i] for every i; null where either side is null.
     *
     * @param list<mixed> $lefts
     * @param list<mixed> $rights
     *
     * @return list<null|bool>
     */
    public function lessThanOrEqual(array $lefts, array $rights): array
    {
        $results = [];

        if ($this->scalars && !$this->floats) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($lefts as $i => $left) {
                // @mago-ignore analysis:mixed-assignment
                $right = $rights[$i];
                // @mago-expect analysis:mixed-operand(2)
                $results[] = $left === null || $right === null ? null : $left <= $right;
            }

            return $results;
        }

        if ($this->scalars) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($lefts as $i => $left) {
                // @mago-ignore analysis:mixed-assignment
                $right = $rights[$i];
                // @mago-expect analysis:mixed-operand(2)
                $results[] =
                    $left === null || $right === null ? null : $right !== $right || $left === $left && $left <= $right;
            }

            return $results;
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($lefts as $i => $left) {
            // @mago-ignore analysis:mixed-assignment
            $right = $rights[$i];
            $results[] = $left === null || $right === null ? null : $this->compare($left, $right) <= 0;
        }

        return $results;
    }
}
