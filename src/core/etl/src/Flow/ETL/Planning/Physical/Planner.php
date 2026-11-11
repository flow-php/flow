<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Physical;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Foundation\ProgressStages;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\VoidProgress;
use Flow\ETL\IO\Extractor\InfersSchema;
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
    public function plan(LogicalPlan $logical, ProgressStages $stages = new VoidProgress()): PhysicalPlan
    {
        $planned = new PlannedNodes($stages);

        // the sources as the frame holds them, before the optimizer rebuilds any: the run refreshes them when it ends
        $refreshed = [];
        $inferring = [];

        foreach ($logical->reads() as $read) {
            $extractor = $read->extractor();

            if ($extractor instanceof Refreshable) {
                $refreshed[] = $extractor;
            }

            if ($extractor instanceof InfersSchema) {
                $inferring[] = $extractor;
            }
        }

        $inference = $stages->stage(StageName::infer);

        foreach ($inferring as $extractor) {
            $extractor->reportInferenceTo($inference);
        }

        try {
            $logical = $this->optimize($logical);
            $onSpine = self::spineNodes($logical);

            // the root's own write is planned on its way down; every other write off the spine right after its host
            $consumer = $logical->root instanceof Node\Outputs ? $logical->root->children()[0] : $logical->root;

            foreach ($logical->writesOnSpine() as $root) {
                foreach ($root instanceof Node\Transaction ? $root->children() : [$root] as $write) {
                    if ($onSpine->offsetExists($write)) {
                        continue;
                    }

                    $host = $write->children()[0];

                    while (!$onSpine->offsetExists($host) && $host->children() !== []) {
                        $host = $host->children()[0];
                    }

                    $planned->hosts($host, $write, $write !== $consumer);
                }
            }

            $this->node($logical->root, $planned);

            return $this->pipelines($logical, $planned)->refreshingAfterRun(...$refreshed);
        } finally {
            $inference->done();

            foreach ($inferring as $extractor) {
                $extractor->reportInferenceTo(new VoidProgress());
            }
        }
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

        if ($node instanceof Node\Read) {
            $planned->planRead($node);
        }

        $inputs = [];

        foreach ($node instanceof Node\JoinsFrame ? [$node->children()[0]] : $node->children() as $child) {
            $inputs[] = $this->node($child, $planned);
        }

        $right = null;

        if ($node instanceof Node\JoinsFrame) {
            // the joined frame runs as a pipeline of its own, so a node it shares with this plan needs its own steps
            $plannedRight = $planned->joined();
            $this->node($node->right(), $plannedRight);
            $right = $this->pipelines(new LogicalPlan($node->right()), $plannedRight);
        }

        $steps = [];

        foreach ($this->translator->steps($node) as $step) {
            if ($node instanceof Node\Write && $step instanceof Loader) {
                $planned->planWrite($step);
            }

            if ($node instanceof Node\JoinEach && $step instanceof Transformer) {
                $planned->planJoinEach($step, $this->translator->joinSubject($node->type, $node->on));
            }

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

        $planned->add($node, new PlannedNode($steps, $bound, $schema));

        foreach ($planned->hostedBy($node) as [$write, $plannedHere]) {
            $planned->planWrite($write->loader);

            if ($plannedHere) {
                $this->node($write, $planned);
            }
        }

        return $planned->of($node);
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
        $onSpine = self::spineNodes($logical);
        $spine = [];

        foreach ($onSpine as $node) {
            $spine[] = $node;
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

        $pipelines = self::cut($logical->source(), $steps, $writePlanner->next(), $planned);

        return new PhysicalPlan(
            $pipelines[count($pipelines) - 1],
            $planned->of($logical->root)->schema,
            $planned->refusal(),
            $logical->root instanceof Node\Write,
        );
    }

    /**
     * The spine's nodes, root first.
     *
     * @return SplObjectStorage<Node, Node>
     */
    public static function spineNodes(LogicalPlan $logical): SplObjectStorage
    {
        /** @var SplObjectStorage<Node, Node> $onSpine */
        $onSpine = new SplObjectStorage();

        // an Outputs below the root carries its own writes; its first consumer is not a step of the spine
        for (
            $node = $logical->spine();;
            $node = $node instanceof Node\Outputs ? $node->children()[0]->children()[0] : $node->children()[0]
        ) {
            $onSpine[$node] = $node;

            if ($node->children() === []) {
                break;
            }
        }

        return $onSpine;
    }

    /**
     * $steps cut after every breaker - a processor that breaks, a hash join - into pipelines numbered from $firstId,
     * each reading the one before it; the first reads $input, or is fed batch by batch when $input is null.
     *
     * @param list<JoinStep|Loader|Processor|BranchPipeline|TransactionGroup|Transformer> $steps
     *
     * @return non-empty-list<Pipeline> leaf first
     */
    public static function cut(?Read $input, array $steps, int $firstId, PlannedNodes $planned): array
    {
        $pipelines = [];
        $current = [];
        $breaker = null;
        $from = $input;

        foreach ($steps as $step) {
            if ($breaker !== null) {
                $from = new Pipeline(
                    $firstId + count($pipelines),
                    $from,
                    new PipelineSteps($current, $breaker, $planned->stages(), $planned->writes()),
                    $from instanceof Read ? $planned->readStageOf($from) : null,
                );
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

        $pipelines[] = new Pipeline(
            $firstId + count($pipelines),
            $from,
            new PipelineSteps($current, $breaker, $planned->stages(), $planned->writes()),
            $from instanceof Read ? $planned->readStageOf($from) : null,
        );

        return $pipelines;
    }
}
