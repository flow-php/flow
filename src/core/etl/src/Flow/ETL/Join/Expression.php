<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Join\Comparison\All;
use Flow\ETL\Join\Comparison\Identical;
use Flow\ETL\Row;
use Flow\Serializer\Serializable;

/**
 * @implements Serializable<array{comparison: Comparison, prefix: string}>
 *
 * @psalm-immutable
 */
final class Expression implements Serializable
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

    public function __serialize() : array
    {
        return [
            'comparison' => $this->comparison,
            'prefix' => $this->joinPrefix,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->comparison = $data['comparison'];
        $this->joinPrefix = $data['prefix'];
    }

    /**
     * @return array<string>
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
     * @return array<string>
     */
    public function right() : array
    {
        return $this->comparison->right();
    }
}
