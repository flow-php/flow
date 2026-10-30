<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Physical;

use Flow\ETL\Planning\Logical\Node\Read;

final readonly class Pipeline
{
    /**
     * @param int $id planner-assigned counter, unique inside the PhysicalPlan it belongs to. A join's right side
     *                is numbered inside the physical plan built for it.
     * @param null|Pipeline|Read $input the read this pipeline starts from, or the pipeline whose output is its rows; null
     *                                  when the branch holding it feeds it batch by batch
     * @param PipelineSteps $steps bound where the schema was derivable, raw where it was not
     */
    public function __construct(
        public int $id,
        public Read|self|null $input,
        public PipelineSteps $steps,
    ) {}

    /**
     * This pipeline and every pipeline it reads, leaf first.
     *
     * @return non-empty-list<self>
     */
    public function chain(): array
    {
        return $this->input instanceof self ? [...$this->input->chain(), $this] : [$this];
    }

    /**
     * The pipeline this one reads, cut off at its breaker; null when this pipeline reads a source.
     */
    public function input(): ?self
    {
        return $this->input instanceof self ? $this->input : null;
    }
}
