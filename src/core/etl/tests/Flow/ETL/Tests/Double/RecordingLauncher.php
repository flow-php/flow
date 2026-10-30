<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\SplittableExtractor;
use Flow\ETL\IO\Inference\Sniffer;
use Flow\ETL\Planning\Physical\PhysicalPlan;
use Flow\ETL\Runtime\Executor\SourceCounts;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Runtime\Launcher\Launch;
use Flow\ETL\Runtime\Launcher\Launcher;

/**
 * Records every read() and sniff() call and hands each to the delegate.
 */
final class RecordingLauncher implements Launcher
{
    /**
     * @var list<array{PhysicalPlan, SplittableExtractor, Criteria, FlowContext, null|SourceCounts}>
     */
    public array $reads = [];

    /**
     * @var list<array{Sniffer, int}>
     */
    public array $sniffs = [];

    /**
     * @param null|Launcher $delegate null: launches nothing, so every plan runs in the executor
     */
    public function __construct(
        private readonly ?Launcher $delegate = null,
    ) {}

    public function read(
        PhysicalPlan $plan,
        SplittableExtractor $source,
        Criteria $criteria,
        FlowContext $context,
        ?SourceCounts $sources = null,
    ): ?Launch {
        $this->reads[] = [$plan, $source, $criteria, $context, $sources];

        return $this->delegate?->read($plan, $source, $criteria, $context, $sources);
    }

    public function sniff(Sniffer $sniffer, int $units): ?array
    {
        $this->sniffs[] = [$sniffer, $units];

        return $this->delegate?->sniff($sniffer, $units);
    }
}
