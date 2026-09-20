<?php

declare(strict_types=1);

namespace Flow\ETL\Executor;

use Flow\ETL\FlowContext;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;

final readonly class Pipeline
{
    /**
     * @param int $id planner-assigned counter, unique inside the PhysicalPlan it belongs to. A join's right side
     *                is numbered inside the physical plan built for it.
     * @param Segments $segments bound where the schema was derivable, raw where it was not. The source
     *                           extractor lives in its first Segment and nowhere else.
     * @param FlowContext $context the frame this pipeline was planned in
     * @param null|self $input the pipeline whose output is this one's rows; null when this pipeline
     *                         reads the extractor in $segments
     * @param null|int<1, max> $limit handed to the source extractor when this pipeline reads it; meaningless
     *                                behind an input edge
     * @param Filter $pathFilter handed to a FileExtractor source when this pipeline reads it; meaningless
     *                           behind an input edge
     */
    public function __construct(
        public int $id,
        private Segments $segments,
        private FlowContext $context,
        private ?self $input = null,
        private ?int $limit = null,
        private Filter $pathFilter = new OnlyFiles(),
    ) {}

    public function segments(): Segments
    {
        return $this->segments;
    }

    public function context(): FlowContext
    {
        return $this->context;
    }

    /**
     * @return null|int<1, max>
     */
    public function limit(): ?int
    {
        return $this->limit;
    }

    public function pathFilter(): Filter
    {
        return $this->pathFilter;
    }

    /**
     * The upstream stage, cut off after a blocking node. The Executor flattens the chain.
     */
    public function input(): ?self
    {
        return $this->input;
    }
}
