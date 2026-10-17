<?php

declare(strict_types=1);

namespace Flow\ETL\Executor;

use Flow\ETL\Extractor\File\FileScan;
use Flow\ETL\FlowContext;

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
     * @param FileScan $scan handed to a FileExtractor source (only its limit to any other source) when this
     *                       pipeline reads it; meaningless behind an input edge
     * @param null|SourceRows $sources counts the rows of the source when this pipeline reads it and the run is
     *                                 analyzed with source statistics; null otherwise
     */
    public function __construct(
        public int $id,
        private Segments $segments,
        private FlowContext $context,
        private ?self $input = null,
        private FileScan $scan = new FileScan(),
        private ?SourceRows $sources = null,
    ) {}

    public function segments(): Segments
    {
        return $this->segments;
    }

    public function context(): FlowContext
    {
        return $this->context;
    }

    public function scan(): FileScan
    {
        return $this->scan;
    }

    public function sources(): ?SourceRows
    {
        return $this->sources;
    }

    /**
     * The upstream stage, cut off after a blocking node. The Executor flattens the chain.
     */
    public function input(): ?self
    {
        return $this->input;
    }
}
