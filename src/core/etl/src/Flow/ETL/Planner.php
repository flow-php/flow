<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Executor\PhysicalPlan;
use Flow\ETL\Extractor\NestedPlan;
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
     * The optimizer rewrites the plan first, so the one walk that translates and binds sees the final tree. An embedded
     * plan - a SideInput's subtree, a NestedPlan a Read inlines - is planned by its own frame's planner into the same
     * $planned: its own rules apply, its pipelines are numbered inside the sub-plan built for it, and a schema
     * refusal is plan-wide.
     * A failure is reported as a started and failed DataFrame span of $context, then rethrown.
     */
    public function plan(
        LogicalPlan $logical,
        FlowContext $context,
        PlannedNodes $planned = new PlannedNodes(),
    ): PhysicalPlan {
        try {
            $logical = $this->optimizer->optimize($logical, $context);

            $this->node($logical->root, $context, $planned);

            return (new PipelineSplit())->of($logical, $planned, $context);
        } catch (Throwable $e) {
            // a failure is never reported without a start
            $context->telemetry()->dataFrameStarted($context);
            $context->telemetry()->dataFrameFailed($context, $e);

            throw $e;
        }
    }

    /**
     * Plans $node and everything under it, each node once by identity: translated to steps, then bound to the
     * schema of its input. A prefix several consumers share is planned once.
     */
    public function node(Node $node, FlowContext $context, PlannedNodes $planned): PlannedNode
    {
        if ($planned->has($node)) {
            return $planned->of($node);
        }

        if ($node instanceof Node\SideInput) {
            $embedded = $node->plan();
            $nested = $embedded->context->config->planner()->plan($embedded->logical, $embedded->context, $planned);

            try {
                $schema = $nested->schema();
            } catch (SchemaNotDerivableException) {
                $schema = null;
            }

            return $planned->add($node, new PlannedNode([], [], $schema, $nested));
        }

        $inputs = [];

        foreach ($node->children() as $child) {
            $inputs[] = $this->node($child, $context, $planned);
        }

        $nested = null;

        if ($node instanceof Node\Read) {
            $extractor = $node->extractor();

            if ($extractor instanceof NestedPlan && $extractor->declaredSchema() === null) {
                $embedded = $extractor->plan($node->limit());
                $nested = $embedded->context->config->planner()->plan($embedded->logical, $embedded->context, $planned);
            }
        }

        $frames = [];

        foreach ($node->children() as $child) {
            if ($child instanceof Node\SideInput) {
                $frames[] = $planned->of($child)->nestedOrFail();
            }
        }

        $steps = NodeTranslator::toSteps($node, $context, $frames);
        $bound = [];
        $schema = null;

        try {
            $schema = match (true) {
                $nested !== null && $node instanceof Node\Read => $nested->schema(),
                $node instanceof Node\Read => $node->schema(),
                $inputs === [] => null,
                default => $inputs[0]->schema,
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

        return $planned->add($node, new PlannedNode($steps, $bound, $schema, $nested));
    }
}
