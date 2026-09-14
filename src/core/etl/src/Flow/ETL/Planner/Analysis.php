<?php

declare(strict_types=1);

namespace Flow\ETL\Planner;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Execution\Run;
use Flow\ETL\Extractor\NestedPlan;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan;
use Flow\ETL\Plan\Described;
use Flow\ETL\Plan\FrameOutput;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Frame;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Refusal;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;
use SplObjectStorage;

use function array_values;

final class Analysis
{
    /**
     * @var SplObjectStorage<Node, Analyzed>
     */
    private SplObjectStorage $analyzed;

    private ?Refusal $refusal = null;

    /**
     * @var list<Rule>
     */
    private readonly array $rules;

    public function __construct(
        private readonly Lowerings $lowerings,
        private readonly PipelineSplit $split,
        Rule ...$rules,
    ) {
        $this->rules = array_values($rules);
        /** @var SplObjectStorage<Node, Analyzed> $analyzed */
        $analyzed = new SplObjectStorage();
        $this->analyzed = $analyzed;
    }

    /**
     * The rules rewrite the plan first, so the one walk that lowers and binds sees the final tree. An embedded
     * plan - a Frame's subtree, a NestedPlan a Read inlines - goes through the same method, so it is planned
     * exactly like a root plan and its pipelines are numbered inside the sub-Plan built for it. The root is a Result or a
     * SinkMultiple, so one walk analyses every consumer; a prefix they share is analysed once, by identity.
     */
    public function plan(LogicalPlan $logical, FlowContext $context): Plan
    {
        foreach ($this->rules as $rule) {
            $logical = $rule->apply($logical, $context);
        }

        $this->of($logical->root, $context);

        return $this->split->of($logical, $this, $context);
    }

    public function of(Node $node, FlowContext $context): Analyzed
    {
        if ($this->analyzed->contains($node)) {
            return $this->analyzed[$node];
        }

        if ($node instanceof Frame) {
            $nested = $this->plan($node->plan(), $node->context());

            return $this->analyzed[$node] = new Analyzed(
                [],
                [],
                $nested instanceof Described ? $nested->schema : null,
                $nested,
            );
        }

        $inputs = [];

        foreach ($node->children() as $child) {
            $inputs[] = $this->of($child, $context);
        }

        $nested = null;

        if ($node instanceof Read) {
            $extractor = $node->extractor();

            if ($extractor instanceof NestedPlan && $extractor->declaredSchema() === null) {
                $nested = $this->plan($extractor->snapshot()->plan, $extractor->snapshot()->context);
            }
        }

        $frames = [];

        foreach ($node->children() as $child) {
            if ($child instanceof Frame) {
                $side = $this->of($child, $context)->nestedOrFail();
                $frames[] = new FrameOutput($side, Run::in($side->root()->context()->config));
            }
        }

        $steps = $this->lowerings->of($node)->steps($node, $context, $frames);
        $bound = [];
        $schema = null;

        try {
            $schema = match (true) {
                $nested !== null && $node instanceof Read => $nested instanceof Described
                    ? $nested->schema
                    : throw $nested->why->toException(),
                $node instanceof Read => $node->schema(),
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
            $this->refusal ??= Refusal::of($refusal);
            $schema = null;
            $bound = $steps;
        }

        return $this->analyzed[$node] = new Analyzed($steps, $bound, $schema, $nested);
    }

    /**
     * null when this node has never been analysed (distinct from an Analyzed with a null $schema).
     */
    public function analyzed(Node $node): ?Analyzed
    {
        return $this->analyzed->contains($node) ? $this->analyzed[$node] : null;
    }

    /**
     * The first refusal seen in this plan. Non-null means the WHOLE plan runs raw - all or nothing. A nested plan
     * analysed after a refusing sibling carries the OUTER refusal, by design.
     */
    public function refusal(): ?Refusal
    {
        return $this->refusal;
    }

    /**
     * The steps a pipeline runs for $node: the bound ones, or the raw ones when any node of the plan refused a
     * schema - a refusal is plan-wide, so every node answers the same way.
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context): array
    {
        $analyzed = $this->of($node, $context);

        return $this->refusal === null ? $analyzed->bound : $analyzed->steps;
    }
}
