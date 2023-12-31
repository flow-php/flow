<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Join\Comparison\All;
use Flow\ETL\Join\Comparison\Identical;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;

final class Expression
{
    public function __construct(
        private readonly Comparison $comparison,
        private readonly string $joinPrefix = ''
    ) {
    }

    /**
     * @psalm-suppress DocblockTypeContradiction
     *
     * @param array<string, string>|Comparison $comparison
     */
    public static function on(array|Comparison $comparison, string $joinPrefix = '') : self
    {
        if (\is_array($comparison)) {
            /** @var array<Comparison> $comparisons */
            $comparisons = [];

            foreach ($comparison as $left => $right) {
                if (!\is_string($left)) {
                    throw new RuntimeException('Expected left entry name to be string, got ' . \gettype($left) . ". Example: ['id' => 'id']");
                }

                if (!\is_string($right)) {
                    throw new RuntimeException('Expected right entry name to be string, got ' . \gettype($right) . ". Example: ['id' => 'id']");
                }

                $comparisons[] = new Identical($left, $right);
            }

            return new self(new All(...$comparisons), $joinPrefix);
        }

        return new self($comparison, $joinPrefix);
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
