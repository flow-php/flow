<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Explain;

use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Physical\JoinStep;
use Flow\ETL\Planning\Physical\PhysicalPlan;
use Flow\ETL\Planning\Physical\Pipeline;

use function array_map;
use function implode;

final readonly class PhysicalOutline
{
    public function __construct(
        private StepDetails $steps = new StepDetails(),
        private NodeDetails $details = new NodeDetails(),
    ) {}

    /**
     * The plan the executor runs: one entry per pipeline, each reading the one under it. A pipeline ends at the
     * processor that needs every row before it emits, so the split is where rows stop flowing through.
     */
    public function of(PhysicalPlan $plan): Entry
    {
        return new Entry($plan, 'Physical plan', $this->schema($plan), null, false, [$this->pipeline($plan->root())]);
    }

    public function pipeline(Pipeline $pipeline): Entry
    {
        $lines = [];
        $children = [];

        if ($pipeline->input instanceof Read) {
            foreach ($this->steps->lines($pipeline->input->extractor()) as $line) {
                $lines[] = $line;
            }

            foreach ($this->source($pipeline->input) as $line) {
                $lines[] = $line;
            }
        }

        $breaker = $pipeline->steps->breaker;

        foreach ([...$pipeline->steps->steps, ...($breaker === null ? [] : [$breaker])] as $step) {
            foreach ($this->steps->lines($step) as $line) {
                $lines[] = $line;
            }

            if ($step instanceof JoinStep) {
                $children[] = $this->joined($step->right);
            }
        }

        $input = $pipeline->input();

        return new Entry(
            $pipeline,
            'Pipeline #' . $pipeline->id,
            $lines,
            null,
            false,
            $input === null ? $children : [$this->pipeline($input), ...$children],
        );
    }

    /**
     * A joined frame is planned apart, so its pipelines are numbered apart too - the name says which side they are.
     */
    public function joined(PhysicalPlan $right): Entry
    {
        $root = $this->pipeline($right->root());

        return new Entry($right, 'Right side: ' . $root->name, $root->lines, null, false, $root->children);
    }

    /**
     * What the extractor was handed.
     *
     * @return list<string>
     */
    public function source(Read $read): array
    {
        return array_map(static fn(string $line): string => '   ' . $line, $this->details->criteria($read->criteria()));
    }

    /**
     * @return list<string>
     */
    public function schema(PhysicalPlan $plan): array
    {
        $refusal = $plan->refusal();

        if ($refusal !== null) {
            return ['Schema: not derivable - ' . $refusal->getMessage()];
        }

        return ['Columns: ' . implode(', ', $plan->schema()->names())];
    }
}
