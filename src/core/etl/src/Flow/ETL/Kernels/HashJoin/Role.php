<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\HashJoin;

use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\Join\Side;
use Flow\ETL\Values\Rows;

/**
 * Which side a hash join builds its table from: the other side is probed batch by batch. The output keeps left then
 * right whatever was built.
 */
final readonly class Role
{
    private function __construct(
        private Side $built,
        private bool $tracksUnmatched,
    ) {}

    public static function buildLeft(JoinType $type): self
    {
        return new self(Side::left, $type === JoinType::left || $type === JoinType::left_anti);
    }

    public static function buildRight(JoinType $type): self
    {
        return new self(Side::right, $type === JoinType::right);
    }

    public function built(): Side
    {
        return $this->built;
    }

    public function merge(RowMerger $merger, Rows $probe, Rows $build): Rows
    {
        return $this->built === Side::right ? $merger->merge($probe, $build) : $merger->merge($build, $probe);
    }

    /**
     * The built side keeps its rows no probe matched: the join outputs them after the last probe batch.
     */
    public function tracksUnmatched(): bool
    {
        return $this->tracksUnmatched;
    }
}
