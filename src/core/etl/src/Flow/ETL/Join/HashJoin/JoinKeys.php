<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use Flow\ETL\Join\Comparison;
use Flow\ETL\Join\Comparison\All;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Comparison\Identical;
use Flow\ETL\Row\Reference;

final readonly class JoinKeys
{
    /**
     * @param list<Reference> $leftRefs
     * @param list<Reference> $rightRefs
     */
    private function __construct(
        private array $leftRefs,
        private array $rightRefs,
    ) {}

    /**
     * @return null|self null when no equality can be extracted (pure non-equality join)
     */
    public static function fromComparison(Comparison $comparison): ?self
    {
        $pairs = self::pairs($comparison);

        if ($pairs === null) {
            return null;
        }

        $uniquePairs = [];

        foreach ($pairs as $pair) {
            $uniquePairs[$pair[0]->name() . "\x00" . $pair[1]->name()] = $pair;
        }

        $leftRefs = [];
        $rightRefs = [];

        foreach ($uniquePairs as [$leftRef, $rightRef]) {
            $leftRefs[] = $leftRef;
            $rightRefs[] = $rightRef;
        }

        return new self($leftRefs, $rightRefs);
    }

    /**
     * @return list<Reference>
     */
    public function leftRefs(): array
    {
        return $this->leftRefs;
    }

    /**
     * @return list<Reference>
     */
    public function rightRefs(): array
    {
        return $this->rightRefs;
    }

    /**
     * @return null|list<array{Reference, Reference}>
     */
    private static function pairs(Comparison $comparison): ?array
    {
        if ($comparison instanceof Equal || $comparison instanceof Identical) {
            return [[$comparison->left()[0], $comparison->right()[0]]];
        }

        if ($comparison instanceof All) {
            $pairs = [];

            foreach ($comparison->comparisons() as $child) {
                // children without extractable equalities are verified on candidate pairs instead
                $pairs = [...$pairs, ...(self::pairs($child) ?? [])];
            }

            return $pairs === [] ? null : $pairs;
        }

        return null;
    }
}
