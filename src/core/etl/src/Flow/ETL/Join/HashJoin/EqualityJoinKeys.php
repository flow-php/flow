<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use DateTimeInterface;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Join\Comparison;
use Flow\ETL\Join\Comparison\All;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Comparison\Identical;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Stringable;

use function is_bool;
use function is_numeric;
use function is_object;
use function is_string;
use function serialize;

final readonly class EqualityJoinKeys implements JoinKeys
{
    /**
     * @param array<Reference> $leftRefs
     * @param array<Reference> $rightRefs
     */
    private function __construct(
        private array $leftRefs,
        private array $rightRefs,
    ) {}

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

    public static function normalize(mixed $value): string
    {
        if ($value === null) {
            return 'null';
        }

        if (is_bool($value)) {
            return 'b:' . ($value ? '1' : '0');
        }

        if (is_numeric($value)) {
            $float = (float) $value;

            // -0.0 == 0.0 but casts to the string "-0"
            return 'n:' . ($float === 0.0 ? '0' : (string) $float);
        }

        if (is_string($value)) {
            return 's:' . $value;
        }

        if ($value instanceof DateTimeInterface) {
            return 'd:' . $value->format('U.u');
        }

        if (is_object($value)) {
            return $value instanceof Stringable
                ? 'o:' . $value::class . ':' . $value->__toString()
                : 'o:' . serialize($value);
        }

        return 'a:' . serialize($value);
    }

    public function leftHash(Row $row): string
    {
        return $this->hash($row, $this->leftRefs);
    }

    public function rightHash(Row $row): string
    {
        return $this->hash($row, $this->rightRefs);
    }

    /**
     * @param array<Reference> $refs
     */
    private function hash(Row $row, array $refs): string
    {
        $parts = [];

        foreach ($refs as $ref) {
            $parts[] = self::normalize($row->valueOf($ref));
        }

        return NativePHPHash::xxh128(serialize($parts));
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
                $childPairs = self::pairs($child);

                if ($childPairs === null) {
                    return null;
                }

                $pairs = [...$pairs, ...$childPairs];
            }

            return $pairs;
        }

        return null;
    }
}
