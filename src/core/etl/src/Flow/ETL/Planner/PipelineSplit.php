<?php

declare(strict_types=1);

namespace Flow\ETL\Planner;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Extractor\Scan;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\Segments;
use Flow\ETL\Plan;
use Flow\ETL\Plan\Described;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Frame;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\SinkMultiple;
use Flow\ETL\Plan\Pipeline;
use Flow\ETL\Plan\Raw;
use SplObjectStorage;

use function array_reverse;

final readonly class PipelineSplit
{
    /**
     * Walks the row-input spine leaf-first and cuts a pipeline after every blocking node.
     *
     * @param LogicalPlan $logical a whole frame's plan, or a Frame's
     * @param Analysis $analysis every node of the plan already analysed
     * @param FlowContext $context the frame this plan belongs to
     *
     * @throws InvalidLogicException when the spine's leaf is not a Read, or a sink shares no node with the spine
     */
    public function of(LogicalPlan $logical, Analysis $analysis, FlowContext $context): Plan
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
        // Result and SinkMultiple add no steps: a blocking node right under them ends the root pipeline itself
        $top = $root;

        while ($top instanceof Result || $top instanceof SinkMultiple) {
            $top = $top->children()[0];
        }

        $input = $analysis->of($spine[0], $context)->nested?->root();
        $scan = new Scan();

        if ($input !== null) {
            $segments = new Segments();
        } elseif ($spine[0] instanceof Read) {
            $segments = new Segments($spine[0]->extractor());
            $scan = $spine[0]->scan();
        } else {
            throw InvalidLogicException::pipelineWithoutSource($spine[0]::class);
        }

        $frames = [];
        $attachment = new SinkAttachment($analysis, $context);
        $remembered = $attachment->attach($logical->sinkRoots(), $onSpine);
        $id = $attachment->next();

        foreach ($spine as $node) {
            foreach ($analysis->steps($node, $context) as $step) {
                $segments->add($step);
            }

            foreach ($remembered->contains($node) ? $remembered[$node] : [] as $step) {
                $segments->add($step);
            }

            foreach ($node->children() as $child) {
                if ($child instanceof Frame) {
                    $frames[] = $analysis->of($child, $context)->nestedOrFail()->root();
                }
            }

            if ($node->materialization() === Materialization::blocking && $node !== $top) {
                $input = new Pipeline($id++, $segments, $context, $input, $frames, $scan);
                $segments = new Segments();
                $frames = [];
                $scan = new Scan();
            }
        }

        $pipeline = new Pipeline($id, $segments, $context, $input, $frames, $scan);
        $refusal = $analysis->refusal();

        return $refusal === null
            ? new Described(
                $pipeline,
                $analysis->of($root, $context)->schema ?? throw InvalidLogicException::because(
                    'A node without a schema requires a plan-wide refusal',
                ),
            )
            : new Raw($pipeline, $refusal);
    }
}
