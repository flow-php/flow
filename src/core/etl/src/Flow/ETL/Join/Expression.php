<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Join\Comparison\{All, Equal};
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;

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
    public static function on(array|Comparison $comparison, string $joinPrefix = 'joined_') : self
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
