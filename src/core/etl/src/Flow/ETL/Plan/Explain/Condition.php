<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

use Flow\ETL\Join\Comparison;
use Flow\ETL\Join\Comparison\All;
use Flow\ETL\Join\Comparison\Any;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Comparison\Identical;
use ReflectionClass;

use function array_map;
use function array_values;
use function implode;

final readonly class Condition
{
    public function of(Comparison $comparison): string
    {
        return match (true) {
            $comparison instanceof All => $this->composite($comparison->comparisons(), ' AND '),
            $comparison instanceof Any => $this->composite($comparison->comparisons(), ' OR '),
            $comparison instanceof Equal => $this->pair($comparison, '='),
            $comparison instanceof Identical => $this->pair($comparison, '==='),
            default => (new ReflectionClass($comparison))->getShortName(),
        };
    }

    /**
     * The conditions an AND joins, each on its own line; anything else stays one line.
     *
     * @return list<string>
     */
    public function lines(Comparison $comparison): array
    {
        return (
            $comparison instanceof All
                ? array_values(array_map(fn(Comparison $each): string => $this->of($each), $comparison->comparisons()))
                : [$this->of($comparison)]
        );
    }

    /**
     * A comparison that joins others is parenthesized, so the operators it sits between stay unambiguous.
     *
     * @param array<Comparison> $comparisons
     */
    public function composite(array $comparisons, string $operator): string
    {
        return implode($operator, array_map(fn(Comparison $each): string => $each instanceof All || $each instanceof Any
            ? '(' . $this->of($each) . ')'
            : $this->of($each), $comparisons));
    }

    public function pair(Comparison $comparison, string $operator): string
    {
        $left = $comparison->left();
        $right = $comparison->right();

        return isset($left[0], $right[0])
            ? $left[0]->name() . ' ' . $operator . ' ' . $right[0]->name()
            : (new ReflectionClass($comparison))->getShortName();
    }
}
