<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Logical\Rewrite;
use Flow\ETL\Planning\Logical\TransformUp;
use Flow\ETL\Planning\Optimizer\Rule;
use Flow\ETL\Values\Column\Backend;

final readonly class SpineCopyingRule implements Rewrite, Rule
{
    public function apply(LogicalPlan $plan, Backend $backend): LogicalPlan
    {
        return new LogicalPlan((new TransformUp())->of($plan->root, $this));
    }

    public function of(Node $original, Node $rebuilt): Node
    {
        return $rebuilt instanceof Read ? new Read($rebuilt->extractor(), $rebuilt->criteria()) : $rebuilt;
    }
}
