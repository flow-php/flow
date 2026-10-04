<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use Flow\ETL\Column\ComparableValues;
use Flow\ETL\Row\NullsOrder;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Native\StringType;

use function array_fill;
use function array_keys;
use function array_multisort;
use function array_push;
use function Flow\Types\DSL\type_bare;
use function is_bool;
use function is_float;
use function is_nan;
use function is_object;
use function strcmp;

use const SORT_ASC;
use const SORT_DESC;
use const SORT_NUMERIC;
use const SORT_REGULAR;
use const SORT_STRING;

/**
 * The one row order of sortBy, the external merge and every check of sortedness: nulls are the smallest value
 * (first ascending, last descending) unless a reference places them at the other end, NaN the largest and equal to
 * itself, -0.0 equals 0.0, strings compare by bytes,
 * numbers, datetimes, dates and times by value, false before true. Descending is the exact reverse of ascending; rows
 * equal on every key keep their input order.
 */
final readonly class RowOrder
{
    /**
     * @param list<Reference> $references
     */
    public function __construct(
        private array $references,
    ) {}

    /**
     * @param list<SortKey> $left
     * @param list<SortKey> $right
     *
     * @return int negative when row $i of $left sorts before row $j of $right
     */
    public function compare(array $left, int $i, array $right, int $j): int
    {
        foreach ($left as $position => $key) {
            $other = $right[$position];
            $comparison = $key->ranks[$i] <=> $other->ranks[$j];

            if ($comparison === 0 && $key->ranks[$i] === 1) {
                // @mago-expect analysis:mixed-argument(2),mixed-operand(2)
                $comparison = $key->flag === SORT_STRING
                    ? strcmp($key->values[$i], $other->values[$j]) <=> 0
                    : $key->values[$i] <=> $other->values[$j];
            }

            if ($comparison !== 0) {
                return $key->descending ? -$comparison : $comparison;
            }
        }

        return 0;
    }

    /**
     * The first row in [$from, $count) of a batch sorted in this order that sorts after row $boundIndex of $bound -
     * or at or after it when rows equal to the bound are excluded.
     *
     * @param list<SortKey> $keys
     * @param list<SortKey> $bound
     */
    public function firstAfter(
        array $keys,
        int $from,
        int $count,
        array $bound,
        int $boundIndex,
        bool $includeEqual,
    ): int {
        $low = $from;
        $high = $count;

        while ($low < $high) {
            $middle = ($low + $high) >> 1;
            $comparison = $this->compare($keys, $middle, $bound, $boundIndex);

            if ($comparison < 0 || $comparison === 0 && $includeEqual) {
                $low = $middle + 1;
            } else {
                $high = $middle;
            }
        }

        return $low;
    }

    /**
     * @return list<SortKey> one per reference, in reference order
     */
    public function keys(Rows $rows): array
    {
        $comparable = new ComparableValues();
        $keys = [];

        foreach ($this->references as $reference) {
            $column = $rows->column($reference->base());
            $type = type_bare($column->type());
            $ordered = $comparable->orderedByPhysical($type);
            $flag = self::flag($type);
            $descending = $reference->sort() === SortOrder::DESC;
            // nulls rank below every value, or above NaN when the reference moves them to the other end
            $nullRank = $reference->nulls() === NullsOrder::defaultFor($reference->sort()) ? 0 : 3;
            $ranks = [];
            $values = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($ordered ? $column->physicals() : $column->values() as $value) {
                if ($value === null) {
                    $ranks[] = $nullRank;
                    $values[] = $flag === SORT_STRING ? '' : 0;
                } elseif (is_float($value) && is_nan($value)) {
                    $ranks[] = 2;
                    $values[] = 0;
                } else {
                    $ranks[] = 1;
                    // objects have no order: every one of them ties
                    $values[] = is_bool($value) ? (int) $value : (is_object($value) ? 0 : $value);
                }
            }

            $keys[] = new SortKey($ranks, $values, $flag, $descending);
        }

        return $keys;
    }

    /**
     * The sort() flag of this order for non-null values of $type: strings by bytes, numbers, datetimes, dates, times
     * and bools by their physical form.
     *
     * @param Type<mixed> $type
     */
    public static function flag(Type $type): int
    {
        $bare = type_bare($type);

        return match (true) {
            $bare instanceof StringType => SORT_STRING,
            (new ComparableValues())->orderedByPhysical($bare) => SORT_NUMERIC,
            default => SORT_REGULAR,
        };
    }

    /**
     * @param list<SortKey> $keys of a batch of $count rows
     *
     * @return list<int> the batch's indices in this order
     */
    public function permutation(array $keys, int $count): array
    {
        // @mago-ignore analysis:possibly-invalid-argument
        $positions = array_keys(array_fill(0, $count, true));

        if ($keys === [] || $count < 2) {
            return $positions;
        }

        $arguments = [];

        foreach ($keys as $key) {
            $direction = $key->descending ? SORT_DESC : SORT_ASC;
            array_push($arguments, $key->ranks, $direction, SORT_NUMERIC, $key->values, $direction, $key->flag);
        }

        // rows equal on every key keep their input order
        $arguments[] = &$positions;
        array_multisort(...$arguments);

        return $positions;
    }
}
