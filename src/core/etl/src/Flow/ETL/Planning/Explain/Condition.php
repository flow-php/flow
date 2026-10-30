<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Explain;

use Flow\ETL\Functions\Join\Condition as JoinCondition;
use Flow\ETL\Functions\Join\Condition\All;
use Flow\ETL\Functions\Join\Condition\Any;
use Flow\ETL\Functions\Join\Condition\Equal;
use Flow\ETL\Functions\Join\Condition\Identical;
use Flow\Telemetry\ObjectExtractor;

use function array_map;
use function array_values;
use function implode;

final readonly class Condition
{
    public function of(JoinCondition $comparison): string
    {
        return match (true) {
            $comparison instanceof All => $this->composite($comparison->comparisons(), ' AND '),
            $comparison instanceof Any => $this->composite($comparison->comparisons(), ' OR '),
            $comparison instanceof Equal => $this->pair($comparison, '='),
            $comparison instanceof Identical => $this->pair($comparison, '==='),
            default => ObjectExtractor::shortName($comparison),
        };
    }

    /**
     * The conditions an AND joins, each on its own line; anything else stays one line.
     *
     * @return list<string>
     */
    public function lines(JoinCondition $comparison): array
    {
        return (
            $comparison instanceof All
                ? array_values(array_map(fn(JoinCondition $each): string => $this->of(
                    $each,
                ), $comparison->comparisons()))
                : [$this->of($comparison)]
        );
    }

    /**
     * A comparison that joins others is parenthesized, so the operators it sits between stay unambiguous.
     *
     * @param array<JoinCondition> $comparisons
     */
    public function composite(array $comparisons, string $operator): string
    {
        return implode($operator, array_map(fn(JoinCondition $each): string => $each instanceof All
            || $each instanceof Any
                ? '(' . $this->of($each) . ')'
                : $this->of($each), $comparisons));
    }

    public function pair(JoinCondition $comparison, string $operator): string
    {
        $left = $comparison->left();
        $right = $comparison->right();

        return isset($left[0], $right[0])
            ? $left[0]->name() . ' ' . $operator . ' ' . $right[0]->name()
            : ObjectExtractor::shortName($comparison);
    }
}
