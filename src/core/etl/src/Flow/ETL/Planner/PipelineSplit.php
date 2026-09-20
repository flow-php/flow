<?php

declare(strict_types=1);

namespace Flow\ETL\Planner;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Executor\Described;
use Flow\ETL\Executor\PhysicalPlan;
use Flow\ETL\Executor\Pipeline;
use Flow\ETL\Executor\Raw;
use Flow\ETL\Executor\Segments;
use Flow\ETL\FlowContext;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use SplObjectStorage;

use function array_reverse;

final readonly class PipelineSplit
{
    /**
     * @param LogicalPlan $logical a whole frame's plan, or a join's right side
     * @param PlannedNodes $planned every node of the plan already planned
     * @param FlowContext $context the frame this plan belongs to
     *
     * @throws InvalidLogicException when the spine's leaf is not a Read, or a sink shares no node with the spine
     */
    public function of(LogicalPlan $logical, PlannedNodes $planned, FlowContext $context): PhysicalPlan
    {
        $root = $logical->root;
        $top = $logical->spine();
        $spine = [];
        /** @var SplObjectStorage<Node, Node> $onSpine */
        $onSpine = new SplObjectStorage();

        for ($node = $top;; $node = $node->children()[0]) {
            $spine[] = $node;
            $onSpine[$node] = $node;

            if ($node->children() === []) {
                break;
            }

            if ($node instanceof Node\Outputs) {
                $node = $node->children()[0];
            }
        }

        $spine = array_reverse($spine);
        $read = $logical->source();
        $input = null;
        $segments = new Segments($read->extractor());
        $limit = $read->limit();
        $pathFilter = $read->pathFilter();

        $attachment = new SinkAttachment($planned, $context);
        $remembered = $attachment->attach($logical->sinksOnSpine(), $onSpine);
        $id = $attachment->next();

        foreach ($spine as $node) {
            foreach ($planned->steps($node) as $step) {
                $segments->add($step);
            }

            foreach ($remembered->offsetExists($node) ? $remembered[$node] : [] as $step) {
                $segments->add($step);
            }

            if ($node->materialization() === Materialization::blocking && $node !== $top) {
                $input = new Pipeline($id++, $segments, $context, $input, $limit, $pathFilter);
                $segments = new Segments();
                $limit = null;
                $pathFilter = new OnlyFiles();
            }
        }

        $pipeline = new Pipeline($id, $segments, $context, $input, $limit, $pathFilter);
        $refusal = $planned->refusal();

        return $refusal === null
            ? new Described(
                $pipeline,
                $planned->of($root)->schema ?? throw InvalidLogicException::because(
                    'A node without a schema requires a plan-wide refusal',
                ),
            )
            : new Raw($pipeline, $refusal, $planned->of($root)->schema);
    }
}
