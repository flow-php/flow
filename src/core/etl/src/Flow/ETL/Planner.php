<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Executor\PhysicalPlan;
use Flow\ETL\Executor\SourceRows;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Planner\NodeTranslator;
use Flow\ETL\Planner\PipelineSplit;
use Flow\ETL\Planner\PlannedNode;
use Flow\ETL\Planner\PlannedNodes;
use Throwable;

final readonly class Planner
{
    public function __construct(
        private Optimizer $optimizer = new Optimizer(),
    ) {}

    /**
     * The optimizer rewrites the plan first, so the one walk that translates and binds sees the final tree. A frame
     * this one joined is part of the plan: nothing is planned with another frame's planner.
     * A failure is reported as a started and failed DataFrame span of $context, then rethrown.
     *
     * @param null|SourceRows $sources counts the rows every source yields, a joined frame's included
     */
    public function plan(LogicalPlan $logical, FlowContext $context, ?SourceRows $sources = null): PhysicalPlan
    {
        $planned = new PlannedNodes();

        try {
            $logical = $this->optimizer->optimize($logical, $context);

            $this->node($logical->root, $context, $planned, $sources);

            return (new PipelineSplit())->of($logical, $planned, $context, $sources);
        } catch (Throwable $e) {
            // a failure is never reported without a start
            $context->telemetry()->dataFrameStarted($context);
            $context->telemetry()->dataFrameFailed($context, $e);

            throw $e;
        }
    }

    /**
     * Plans $node and everything under it, each node once by identity: translated to steps, then bound to the
     * schema of its input. A prefix several consumers share is planned once; a joined frame is planned apart.
     */
    public function node(
        Node $node,
        FlowContext $context,
        PlannedNodes $planned,
        ?SourceRows $sources = null,
    ): PlannedNode {
        if ($planned->has($node)) {
            return $planned->of($node);
        }

        $inputs = [];

        foreach ($node instanceof Node\JoinsFrame ? [$node->children()[0]] : $node->children() as $child) {
            $inputs[] = $this->node($child, $context, $planned, $sources);
        }

        $frames = [];

        if ($node instanceof Node\JoinsFrame) {
            // the joined frame runs as a pipeline of its own, so a node it shares with this plan needs its own steps
            $right = new PlannedNodes();
            $this->node($node->right(), $context, $right, $sources);
            $frames[] = (new PipelineSplit())->of(new LogicalPlan($node->right()), $right, $context, $sources);
        }

        $steps = NodeTranslator::toSteps($node, $context, $frames);
        $bound = [];
        $schema = null;

        try {
            $schema = match (true) {
                $node instanceof Node\Read => $node->schema(),
                default => $inputs[0]->schema ?? null,
            };

            if ($schema !== null) {
                foreach ($steps as $step) {
                    if ($step instanceof Loader) {
                        $bound[] = $step;

                        continue;
                    }

                    $boundStep = $step->bind($schema);
                    $schema = $boundStep->output;
                    $bound[] = $boundStep->step;
                }
            }
        } catch (SchemaNotDerivableException $refusal) {
            $planned->refuse($refusal);
            $schema = null;
            $bound = $steps;
        }

        return $planned->add($node, new PlannedNode($steps, $bound, $schema));
    }
}
