<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\SplittableExtractor;
use Flow\ETL\IO\Inference\Sniffer;
use Flow\ETL\Planning\Physical\PhysicalPlan;
use Flow\ETL\Planning\Physical\PipelineSteps;
use Flow\ETL\Runtime\Executor\AdoptedBatches;
use Flow\ETL\Runtime\Executor\RunCounts;
use Flow\ETL\Runtime\Executor\StepRunner;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Runtime\Launcher\Launch;
use Flow\ETL\Runtime\Launcher\Launcher;

/**
 * Launches every plan the same way: $steps run over the source here, the parent resumes with $rest, and every
 * worker in $workers reports its peak.
 */
final readonly class StubLauncher implements Launcher
{
    /**
     * @param int<1, max> $consumed
     * @param array<int, int> $workers the peak bytes of each worker, by pid
     */
    public function __construct(
        private PipelineSteps $steps = new PipelineSteps(),
        private PipelineSteps $rest = new PipelineSteps(),
        private int $consumed = 1,
        private array $workers = [],
    ) {}

    public function read(
        PhysicalPlan $plan,
        SplittableExtractor $source,
        Criteria $criteria,
        FlowContext $context,
        ?RunCounts $counts = null,
    ): Launch {
        $batches = (new AdoptedBatches($context->backend()))->of($source->extract($context->backend(), $criteria));

        foreach ($this->workers as $pid => $peakBytes) {
            $counts?->worker($pid, $peakBytes);
        }

        return new Launch(
            $this->consumed,
            $this->rest,
            (new StepRunner())->steps(
                $this->steps,
                $counts === null ? $batches : $counts->count($source, $batches, $criteria->narrows()),
                $context,
                $source,
            ),
        );
    }

    public function sniff(Sniffer $sniffer, int $units): ?array
    {
        return null;
    }
}
