<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Join\Comparison\{All, Equal};
use Flow\ETL\Row\Reference;
use Flow\ETL\{Row};

final readonly class Expression
{
    public function __construct(
        private Comparison $comparison,
        private string $joinPrefix = '',
    ) {
    }

    /**
     * @param array<Comparison>|array<string, string>|Comparison $comparison
     */
    public static function on(array|Comparison $comparison, string $joinPrefix = '') : self
    {
        if (\is_array($comparison)) {
            /** @var array<Comparison> $comparisons */
            $comparisons = [];

            foreach ($comparison as $left => $right) {
                if ($right instanceof Comparison) {
                    $comparisons[] = $right;

                    continue;
                }

                if (!\is_string($left)) {
                    throw new RuntimeException('Expected left entry name to be string, got ' . \gettype($left) . ". Example: ['id' => 'id']");
                }

                if (!\is_string($right)) {
                    throw new RuntimeException('Expected right entry name to be string, got ' . \gettype($right) . ". Example: ['id' => 'id']");
                }

                $comparisons[] = new Equal($left, $right);
            }

            return new self(new All(...$comparisons), $joinPrefix);
        }

        return new self($comparison, $joinPrefix);
    }

    public function dropDuplicateLeftEntries(Row $left) : Row
    {
        if ($this->joinPrefix === '') {
            $leftEntries = [];
            $rightEntries = [];

            foreach ($this->left() as $leftReference) {
                $leftEntries[] = $leftReference->name();
            }

            foreach ($this->right() as $rightReference) {
                $rightEntries[] = $rightReference->name();
            }

            $dropLeft = [];

            foreach ($leftEntries as $leftEntry) {
                if (\in_array($leftEntry, $rightEntries, true)) {
                    $dropLeft[] = $leftEntry;
                }
            }

            return $left->remove(...$dropLeft);
        }

        return $left;
    }

    public function dropDuplicateRightEntries(Row $right) : Row
    {
        if ($this->joinPrefix === '') {
            $leftEntries = [];
            $rightEntries = [];

            foreach ($this->left() as $leftReference) {
                $leftEntries[] = $leftReference->name();
            }

            foreach ($this->right() as $rightReference) {
                $rightEntries[] = $rightReference->name();
            }

            $dropRight = [];

            foreach ($rightEntries as $rightEntry) {
                if (\in_array($rightEntry, $leftEntries, true)) {
                    $dropRight[] = $rightEntry;
                }
            }

            return $right->remove(...$dropRight);
        }

        return $right;
    }

    /**
     * @return array<Reference>
     */
    public function left() : array
    {
        return $this->comparison->left();
    }

    public function meet(Row $left, Row $right) : bool
    {
        return $this->comparison->compare($left, $right);
    }

    public function prefix() : string
    {
        return $this->joinPrefix;
    }

    /**
     * @return array<Reference>
     */
    public function right() : array
    {
        return $this->comparison->right();
    }
}
