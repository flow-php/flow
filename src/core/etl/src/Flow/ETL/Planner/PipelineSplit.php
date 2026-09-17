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
use Flow\ETL\Plan\Node\SideInput;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use SplObjectStorage;

use function array_reverse;

final readonly class PipelineSplit
{
    /**
     * Walks the row-input spine leaf-first and cuts a pipeline after every blocking node.
     *
     * @param LogicalPlan $logical a whole frame's plan, or a SideInput's
     * @param PlannedNodes $planned every node of the plan already planned
     * @param FlowContext $context the frame this plan belongs to
     *
     * @throws InvalidLogicException when the spine's leaf is not a Read, or a sink shares no node with the spine
     */
    public function of(LogicalPlan $logical, PlannedNodes $planned, FlowContext $context): PhysicalPlan
    {
        $root = $logical->root;
        $spine = [];
        /** @var SplObjectStorage<Node, Node> $onSpine */
        $onSpine = new SplObjectStorage();

        for ($node = $root;; $node = $node->children()[0]) {
            $spine[] = $node;
            $onSpine[$node] = $node;

            if ($node->children() === []) {
                break;
            }
        }

        $spine = array_reverse($spine);
        // Result and Outputs add no steps: a blocking node right under them ends the root pipeline itself
        $top = $logical->cursor();
        $read = $logical->source();
        $input = $planned->of($read)->nested?->root();
        // an inlined frame's pipelines feed the rows; its extractor stays so a failure escaping them is put to this
        // frame's handler, as it is when the frame is read through its extractor
        $segments = new Segments($read->extractor());
        $limit = $input === null ? $read->limit() : null;
        $pathFilter = $input === null ? $read->pathFilter() : new OnlyFiles();

        $attachment = new SinkAttachment($planned, $context);
        $remembered = $attachment->attach($logical->sinks(), $onSpine);
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
