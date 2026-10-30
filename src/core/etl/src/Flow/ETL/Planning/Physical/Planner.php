<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Physical;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\IO\Extractor\Refreshable;
use Flow\ETL\IO\Loader;
use Flow\ETL\Operators\Join\CrossJoin;
use Flow\ETL\Operators\Join\HashJoin;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Optimizer\Optimizer;
use Flow\ETL\Values\Column\Backend;
use SplObjectStorage;

use function array_reverse;
use function count;

final readonly class Planner
{
    public function __construct(
        private Optimizer $optimizer,
        private NodeTranslator $translator,
        private Backend $backend,
    ) {}

    /**
     * The optimizer rewrites the plan first, so the one walk that translates and binds sees the final tree. A frame
     * this one joined is part of the plan: nothing is planned with another frame's planner.
     */
    public function plan(LogicalPlan $logical): PhysicalPlan
    {
        $planned = new PlannedNodes();

        // the sources as the frame holds them, before the optimizer rebuilds any: the run refreshes them when it ends
        $refreshed = [];

        foreach ($logical->reads() as $read) {
            $extractor = $read->extractor();

            if ($extractor instanceof Refreshable) {
                $refreshed[] = $extractor;
            }
        }

        $logical = $this->optimize($logical);

        $this->node($logical->root, $planned);

        return $this->pipelines($logical, $planned)->refreshingAfterRun(...$refreshed);
    }

    public function optimize(LogicalPlan $logical): LogicalPlan
    {
        return $this->optimizer->optimize($logical, $this->backend);
    }

    /**
     * Plans $node and everything under it, each node once by identity: translated to steps, then bound to the
     * schema of its input. A prefix several consumers share is planned once; a joined frame is planned apart.
     */
    public function node(Node $node, PlannedNodes $planned): PlannedNode
    {
        if ($planned->has($node)) {
            return $planned->of($node);
        }

        $inputs = [];

        foreach ($node instanceof Node\JoinsFrame ? [$node->children()[0]] : $node->children() as $child) {
            $inputs[] = $this->node($child, $planned);
        }

        $right = null;

        if ($node instanceof Node\JoinsFrame) {
            // the joined frame runs as a pipeline of its own, so a node it shares with this plan needs its own steps
            $plannedRight = new PlannedNodes();
            $this->node($node->right(), $plannedRight);
            $right = $this->pipelines(new LogicalPlan($node->right()), $plannedRight);
        }

        $steps = [];

        foreach ($this->translator->steps($node) as $step) {
            $steps[] = match (true) {
                !$step instanceof HashJoin && !$step instanceof CrossJoin => $step,
                $right === null => throw InvalidLogicException::because(
                    'A join translates only from a node that joins a frame, %s given',
                    $node::class,
                ),
                // the steps as translated run only when the plan refuses: an unbound hash join still pads the right
                // side's declared columns
                $step instanceof HashJoin && $right->described() => new JoinStep(
                    $step->withRightSchema($right->schema()),
                    $right,
                ),
                default => new JoinStep($step, $right),
            };
        }

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

                    if ($step instanceof JoinStep) {
                        $boundStep = $step->join->bind($schema, $step->right->schema());
                        $schema = $boundStep->output;
                        $bound[] = new JoinStep($boundStep->step, $step->right);

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

    /**
     * The spine's pipelines, cut at every breaker, with every write attached to the spine node it reads.
     *
     * @param LogicalPlan $logical a whole frame's plan, or a join's right side
     * @param PlannedNodes $planned every node of the plan already planned
     *
     * @throws InvalidLogicException when the spine's leaf is not a Read, or a write shares no node with the spine
     */
    public function pipelines(LogicalPlan $logical, PlannedNodes $planned): PhysicalPlan
    {
        $spine = [];
        /** @var SplObjectStorage<Node, Node> $onSpine */
        $onSpine = new SplObjectStorage();

        // an Outputs below the root carries its own writes; its first consumer is not a step of the spine
        for (
            $node = $logical->spine();;
            $node = $node instanceof Node\Outputs ? $node->children()[0]->children()[0] : $node->children()[0]
        ) {
            $spine[] = $node;
            $onSpine[$node] = $node;

            if ($node->children() === []) {
                break;
            }
        }

        $steps = [];
        $writePlanner = new WritePlanner($planned);
        $remembered = $writePlanner->attach($logical->writesOnSpine(), $onSpine);

        foreach (array_reverse($spine) as $node) {
            foreach ($planned->steps($node) as $step) {
                $steps[] = $step;
            }

            foreach ($remembered->offsetExists($node) ? $remembered[$node] : [] as $step) {
                $steps[] = $step;
            }
        }

        $pipelines = self::cut($logical->source(), $steps, $writePlanner->next());

        return new PhysicalPlan(
            $pipelines[count($pipelines) - 1],
            $planned->of($logical->root)->schema,
            $planned->refusal(),
            $logical->root instanceof Node\Write,
        );
    }

    /**
     * $steps cut after every breaker - a processor that breaks, a hash join - into pipelines numbered from $firstId,
     * each reading the one before it; the first reads $input, or is fed batch by batch when $input is null.
     *
     * @param list<JoinStep|Loader|Processor|BranchPipeline|TransactionGroup|Transformer> $steps
     *
     * @return non-empty-list<Pipeline> leaf first
     */
    public static function cut(?Read $input, array $steps, int $firstId): array
    {
        $pipelines = [];
        $current = [];
        $breaker = null;
        $from = $input;

        foreach ($steps as $step) {
            if ($breaker !== null) {
                $from = new Pipeline($firstId + count($pipelines), $from, new PipelineSteps($current, $breaker));
                $pipelines[] = $from;
                $current = [];
                $breaker = null;
            }

            if (
                $step instanceof Processor && $step->breaks()
                || $step instanceof JoinStep && $step->join instanceof HashJoin
            ) {
                $breaker = $step;
            } else {
                $current[] = $step;
            }
        }

        $pipelines[] = new Pipeline($firstId + count($pipelines), $from, new PipelineSteps($current, $breaker));

        return $pipelines;
    }
}
