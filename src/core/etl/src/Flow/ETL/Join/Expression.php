<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Join\Comparison\All;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;

use function array_slice;
use function gettype;
use function is_array;
use function is_string;

final readonly class Expression
{
    public function __construct(
        private Comparison $comparison,
        private string $joinPrefix = '',
    ) {}

    /**
     * @param array<Comparison>|array<string, string>|Comparison $comparison
     */
    public static function on(array|Comparison $comparison, string $joinPrefix = ''): self
    {
        if (is_array($comparison)) {
            /** @var array<Comparison> $comparisons */
            $comparisons = [];

            foreach ($comparison as $left => $right) {
                if ($right instanceof Comparison) {
                    $comparisons[] = $right;

                    continue;
                }

                if (!is_string($left)) {
                    throw new RuntimeException(
                        'Expected left entry name to be string, got ' . gettype($left) . ". Example: ['id' => 'id']",
                    );
                }

                // @mago-ignore analysis:impossible-condition,redundant-type-comparison
                if (!is_string($right)) {
                    // @mago-ignore analysis:no-value
                    throw new RuntimeException(
                        'Expected right entry name to be string, got ' . gettype($right) . ". Example: ['id' => 'id']",
                    );
                }

                $comparisons[] = new Equal($left, $right);
            }

            if ($comparisons === []) {
                throw new RuntimeException('Expected at least one comparison in the join expression.');
            }

            $first = $comparisons[0];
            $rest = array_slice($comparisons, 1);

            return new self(new All($first, ...$rest), $joinPrefix);
        }

        return new self($comparison, $joinPrefix);
    }

    public function comparison(): Comparison
    {
        return $this->comparison;
    }

    public function dropDuplicateLeftEntries(Row $left): Row
    {
        if ($this->joinPrefix !== '') {
            return $left;
        }

        $dropLeft = [];

        foreach ($this->left() as $leftReference) {
            foreach ($this->right() as $rightReference) {
                if ($leftReference->name() === $rightReference->name()) {
                    $dropLeft[] = $leftReference->name();

                    continue 2;
                }
            }
        }

        return $left->remove(...$dropLeft);
    }

    public function dropDuplicateRightEntries(Row $right): Row
    {
        if ($this->joinPrefix !== '') {
            return $right;
        }

        $dropRight = [];

        foreach ($this->right() as $rightReference) {
            foreach ($this->left() as $leftReference) {
                if ($rightReference->name() === $leftReference->name()) {
                    $dropRight[] = $rightReference->name();

                    continue 2;
                }
            }
        }

        return $right->remove(...$dropRight);
    }

    /**
     * @return array<Reference>
     */
    public function left(): array
    {
        return $this->comparison->left();
    }

    public function meet(Row $left, Row $right): bool
    {
        return $this->comparison->compare($left, $right);
    }

    public function prefix(): string
    {
        return $this->joinPrefix;
    }

    /**
     * @return array<Reference>
     */
    public function right(): array
    {
        return $this->comparison->right();
    }
}
