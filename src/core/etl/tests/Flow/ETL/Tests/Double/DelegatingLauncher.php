<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Progress;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\SplittableExtractor;
use Flow\ETL\IO\Inference\Sniffer;
use Flow\ETL\Planning\Physical\PhysicalPlan;
use Flow\ETL\Planning\Physical\PipelineSteps;
use Flow\ETL\Runtime\Executor\RunCounts;
use Flow\ETL\Runtime\Executor\StepRunner;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Runtime\Launcher\Launch;
use Flow\ETL\Runtime\Launcher\Launcher;
use Flow\ETL\Values\Rows;
use Generator;
use Throwable;

/**
 * Hands the source's batches back through two `yield from` levels, as a parallel launch collects its workers' rows;
 * a failure thrown into it may fail its teardown with $teardownFailure.
 */
final readonly class DelegatingLauncher implements Launcher
{
    public function __construct(
        private PipelineSteps $rest = new PipelineSteps(),
        private ?Throwable $teardownFailure = null,
    ) {}

    public function read(
        PhysicalPlan $plan,
        SplittableExtractor $source,
        Criteria $criteria,
        FlowContext $context,
        ?RunCounts $counts = null,
    ): Launch {
        return new Launch(
            1,
            $this->rest,
            $this->delegated($this->delegated((new StepRunner())->steps(
                new PipelineSteps(),
                $source->extract($context->backend(), $criteria),
                $context,
                $source,
            ))),
        );
    }

    /**
     * @param Generator<int, Rows, null|Signal, void> $batches
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function delegated(Generator $batches): Generator
    {
        try {
            yield from $batches;
        } catch (Throwable $failure) {
            throw $this->teardownFailure ?? $failure;
        }
    }

    public function sniff(Sniffer $sniffer, int $units, Progress $progress): ?array
    {
        return null;
    }
}
