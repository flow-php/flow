<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

use Flow\ETL\Plan;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Outputs;
use Flow\ETL\Plan\Node\SideInput;
use Flow\ETL\Plan\Stage;
use SplObjectStorage;

use function count;

final readonly class Outline
{
    /**
     * @param Stage $stage which plan of an embedded frame is shown: as built, or after that frame's own optimizer
     */
    public function __construct(
        private Stage $stage = Stage::unoptimized,
    ) {}

    /**
     * The plan at this outline's stage: optimized with the frame's own optimizer, the one its planner runs.
     */
    public function logical(Plan $plan): LogicalPlan
    {
        return match ($this->stage) {
            Stage::unoptimized => $plan->logical,
            Stage::optimized => $plan->context->config->optimizer()->optimize($plan->logical, $plan->context),
        };
    }

    public function of(Node $root): Entry
    {
        /** @var SplObjectStorage<Node, int> $numbers */
        $numbers = new SplObjectStorage();

        return $this->entry($root, $numbers);
    }

    /**
     * A node is numbered once everything it reads is, so the numbers follow the order rows move: the source is #1.
     *
     * @param SplObjectStorage<Node, int> $numbers the nodes numbered so far
     */
    public function entry(Node $node, SplObjectStorage $numbers): Entry
    {
        if ($numbers->offsetExists($node)) {
            return new Entry($node, $numbers[$node], true, []);
        }

        $children = [];

        // an embedded frame is a leaf of this plan; its own plan is shown under it, numbered with this one
        foreach ($node instanceof SideInput ? [$this->logical($node->plan())->root] : $node->children() as $child) {
            $children[] = $this->entry($child, $numbers);
        }

        if ($node instanceof Outputs) {
            return new Entry($node, null, false, $children);
        }

        $numbers[$node] = $number = count($numbers) + 1;

        return new Entry($node, $number, false, $children);
    }
}
