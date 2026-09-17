<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Plan;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;

/**
 * Another plan read by this one: a join's right side, a cross join's right side. Never children()[0] -
 * a SideInput contributes no rows to the chain it sits in, its rows go to the operator above it.
 */
final readonly class SideInput implements Node
{
    public function __construct(
        private Plan $plan,
    ) {}

    public function plan(): Plan
    {
        return $this->plan;
    }

    /**
     * A leaf: the embedded plan is planned as a whole under its own root, so a rewrite of the outer plan
     * never reaches into it.
     *
     * @return list<Node>
     */
    public function children(): array
    {
        return [];
    }

    public function withChildren(array $children): self
    {
        return $this;
    }

    public function rowCount(): RowCount
    {
        return RowCount::preserving;
    }

    public function transparency(): Transparency
    {
        return Transparency::opaque;
    }

    public function materialization(): Materialization
    {
        return Materialization::streaming;
    }

    public function redefines(): Redefined
    {
        return Redefined::none();
    }
}
