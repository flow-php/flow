<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateInterval;
use DateTimeImmutable;
use Flow\Types\Type;
use Flow\Types\Type\Logical\NumericStringType;

use function Flow\Types\DSL\type_bare;
use function is_string;
use function strcmp;

/**
 * Two strings order by bytes, as sort does, unless an operand is a numeric string column, which orders numerically.
 * Anything else orders as PHP's <=>, so `$a < $b` is `compare($a, $b) < 0` and `$a > $b` is `compare($b, $a) < 0`,
 * NaN included.
 */
final readonly class ValueOrder
{
    private bool $numericStrings;

    /**
     * @param Type<mixed> ...$operands
     */
    public function __construct(Type ...$operands)
    {
        $numericStrings = false;

        foreach ($operands as $operand) {
            $numericStrings = $numericStrings || type_bare($operand) instanceof NumericStringType;
        }

        $this->numericStrings = $numericStrings;
    }

    public function compare(mixed $left, mixed $right): int
    {
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
}
