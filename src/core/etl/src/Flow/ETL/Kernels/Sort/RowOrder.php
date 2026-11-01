<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Sort;

use Flow\ETL\Values\Column\ComparableValues;
use Flow\ETL\Values\NullsOrder;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\SortOrder;
use Flow\Types\Type;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\StringType;

use function array_fill;
use function array_keys;
use function array_multisort;
use function array_push;
use function array_sum;
use function count;
use function Flow\Types\DSL\type_bare;
use function is_bool;
use function is_nan;
use function is_object;
use function strcmp;

use const SORT_ASC;
use const SORT_DESC;
use const SORT_NUMERIC;
use const SORT_REGULAR;
use const SORT_STRING;

/**
 * Nulls smallest unless placed, NaN largest, -0.0 = 0.0, strings by bytes; ties keep input order.
 */
final readonly class RowOrder
{
    /**
     * @param list<SortColumn> $columns
     */
    public function __construct(
        private array $columns,
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
     * @return list<SortKey> one per column, in column order
     */
    public function keys(Rows $rows): array
    {
        $comparable = new ComparableValues();
        $keys = [];

        foreach ($this->columns as $sortColumn) {
            $column = $rows->column($sortColumn->name);
            $flag = self::flag($column->type());
            $descending = $sortColumn->order === SortOrder::DESC;
            // nulls rank below every value, or above NaN when the column moves them to the other end
            $nullRank = $sortColumn->nulls === NullsOrder::defaultFor($sortColumn->order) ? 0 : 3;
            $ranks = [];
            $values = [];
            $raw = $comparable->ordering($column);

            // a column of plain strings or numbers without a null or NaN: every rank is 1 and the values stand as they
            // are, so no per-row loop - a NaN among numbers makes their sum NaN, which is one C-speed pass
            // @mago-ignore analysis:less-specific-nested-argument-type
            if (
                $column->nullCount() === 0
                && ($flag === SORT_STRING || $flag === SORT_NUMERIC && !is_nan(array_sum($raw)))
            ) {
                $keys[] = new SortKey(array_fill(0, count($raw), 1), $raw, $flag, $descending);

                continue;
            }

            // @mago-ignore analysis:mixed-assignment
            foreach ($raw as $value) {
                if ($value === null) {
                    $ranks[] = $nullRank;
                    $values[] = $flag === SORT_STRING ? '' : 0;
                } elseif ($value !== $value) {
                    // NaN is the one value not identical to itself: no is_float()/is_nan() call per row
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
     * The sort() flag of this order for non-null physicals of $type: strings, uuids, json, enums, xml and html by
     * bytes, numbers, datetimes, dates, times and bools by value.
     *
     * @param Type<mixed> $type
     */
    public static function flag(Type $type): int
    {
        $bare = type_bare($type);

        return match (true) {
            $bare instanceof StringType,
            $bare instanceof UuidType,
            $bare instanceof JsonType,
            $bare instanceof EnumType,
            $bare instanceof XMLType,
            $bare instanceof XMLElementType,
            $bare instanceof HTMLType,
            $bare instanceof HTMLElementType,
                => SORT_STRING,
            (new ComparableValues())->equalByPhysical($bare) => SORT_NUMERIC,
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

    /**
     * @return list<bool> per row of a partition sorted in this order: equal to the row before it on every column (row 0
     *                    never is)
     */
    public function ties(Rows $partition): array
    {
        $count = $partition->count();
        $ties = [];

        for ($i = 0; $i < $count; $i++) {
            $ties[] = $i > 0;
        }

        $comparable = new ComparableValues();

        foreach ($this->columns as $sortColumn) {
            $column = $partition->column($sortColumn->name);
            // === on the comparable form is value equality; == on the values of a list, map or structure, so two
            // datetimes of one instant tie and map keys compare in any order
            $physical = $comparable->equalByPhysical($column->type());
            $values = $comparable->equality($column);

            for ($i = 1; $i < $count; $i++) {
                // @mago-expect analysis:mixed-operand(2)
                if ($ties[$i] && ($physical ? $values[$i - 1] !== $values[$i] : $values[$i - 1] != $values[$i])) {
                    $ties[$i] = false;
                }
            }
        }

        return $ties;
    }
}
