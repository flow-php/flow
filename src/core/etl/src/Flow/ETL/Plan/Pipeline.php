<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use Flow\ETL\Extractor\Scan;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\Segments;

final readonly class Pipeline
{
    /**
     * @param int $id planner-assigned counter, unique inside the Plan it belongs to. An embedded plan's
     *                pipelines are numbered inside the sub-Plan built for it.
     * @param Segments $segments bound where the schema was derivable, raw where it was not. The source
     *                           extractor lives in its first Segment and nowhere else.
     * @param FlowContext $context the frame this pipeline was lowered in
     * @param null|self $input the pipeline whose output is this one's rows; null when this pipeline
     *                         reads the extractor in $segments
     * @param list<self> $frames the pipelines this one's operators pull as side inputs
     * @param Scan $scan what the source extractor is handed when this pipeline reads it; meaningless
     *                   behind an input edge
     */
    public function __construct(
        public int $id,
        private Segments $segments,
        private FlowContext $context,
        private ?self $input = null,
        private array $frames = [],
        private Scan $scan = new Scan(),
    ) {}

    public function segments(): Segments
    {
        return $this->segments;
    }

    public function context(): FlowContext
    {
        return $this->context;
    }

    public function scan(): Scan
    {
        return $this->scan;
    }

    /**
     * The upstream stage. NOT necessarily "completes first": under a blocking node it is, under a
     * from_data_frame() edge it streams. The Executor flattens either way.
     */
    public function input(): ?self
    {
        return $this->input;
    }

    /**
     * Side inputs. Each is drained before this pipeline emits its first row, because the operators that
     * own them do that (HashJoinProcessor, CrossJoinRowsTransformer).
     *
     * @return list<self>
     */
    public function frames(): array
    {
        return $this->frames;
    }

    /**
     * @return list<self>
     */
    public function dependencies(): array
    {
        return [...($this->input === null ? [] : [$this->input]), ...$this->frames];
    }
}
