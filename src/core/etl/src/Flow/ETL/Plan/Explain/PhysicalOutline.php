<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

use Flow\ETL\Executor\Described;
use Flow\ETL\Executor\PhysicalPlan;
use Flow\ETL\Executor\Pipeline;
use Flow\ETL\Processor\HashJoinProcessor;
use Flow\ETL\Transformer\CrossJoinRowsTransformer;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function implode;

final readonly class PhysicalOutline
{
    public function __construct(
        private StepDetails $steps = new StepDetails(),
        private Details $details = new Details(),
    ) {}

    /**
     * The plan the executor runs: one entry per pipeline, each reading the one under it. A pipeline ends where a
     * blocking step cuts it, so the split is where rows stop flowing through.
     */
    public function of(PhysicalPlan $plan): Entry
    {
        return new Entry($plan, 'Physical plan', $this->schema($plan), null, false, [$this->pipeline($plan->root())]);
    }

    public function pipeline(Pipeline $pipeline): Entry
    {
        $lines = [];
        $children = [];

        foreach ($pipeline->segments()->all() as $segment) {
            $extractor = $segment->extractor();

            if ($extractor !== null) {
                foreach ($this->steps->lines($extractor) as $line) {
                    $lines[] = $line;
                }

                foreach ($this->source($pipeline) as $line) {
                    $lines[] = $line;
                }
            }

            $processor = $segment->processor();

            foreach ([...$segment->steps(), ...($processor === null ? [] : [$processor])] as $step) {
                foreach ($this->steps->lines($step) as $line) {
                    $lines[] = $line;
                }

                $joined = match (true) {
                    $step instanceof HashJoinProcessor, $step instanceof CrossJoinRowsTransformer => $step->right,
                    default => null,
                };

                if ($joined !== null) {
                    $children[] = $this->joined($joined);
                }
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
     * What the source was handed, when the pipeline reads it directly.
     *
     * @return list<string>
     */
    public function source(Pipeline $pipeline): array
    {
        $lines = [];
        $limit = $pipeline->limit();

        if ($limit !== null) {
            $lines[] = '   Limit: ' . $limit;
        }

        if (!$pipeline->pathFilter() instanceof OnlyFiles) {
            $lines[] = '   Files: ' . $this->details->name($pipeline->pathFilter());
        }

        return $lines;
    }

    /**
     * @return list<string>
     */
    public function schema(PhysicalPlan $plan): array
    {
        if (!$plan instanceof Described) {
            return ['Schema: not derivable - ' . $plan->why->getMessage()];
        }

        return ['Columns: ' . implode(', ', $plan->schema->references()->names())];
    }
}
