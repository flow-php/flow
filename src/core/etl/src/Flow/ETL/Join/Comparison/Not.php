<?php

declare(strict_types=1);

namespace Flow\ETL\Join\Comparison;

use Flow\ETL\Join\Comparison;
use Flow\ETL\Row;

/**
 * @implements Comparison<array{comparison: Comparison}>
 *
 * @psalm-immutable
 */
final class Not implements Comparison
{
    public function __construct(private readonly Comparison $comparison)
    {
    }

    public function __serialize() : array
    {
        return [
            'comparison' => $this->comparison,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->comparison = $data['comparison'];
    }

    public function compare(Row $left, Row $right) : bool
    {
        return !$this->comparison->compare($left, $right);
    }

    /**
     * @return array<string>
     */
    public function left() : array
    {
        return $this->comparison->left();
    }

    /**
     * @return array<string>
     */
    public function right() : array
    {
        return $this->comparison->right();
    }
}
