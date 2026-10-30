<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Launcher;

use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\SplittableExtractor;
use Flow\ETL\IO\Inference\Sniffs;
use Flow\ETL\Planning\Physical\PhysicalPlan;
use Flow\ETL\Runtime\Executor\SourceCounts;
use Flow\ETL\Runtime\FlowContext;

interface Launcher extends Sniffs
{
    /**
     * Decides before the first batch whether launching the head of $plan's chain pays; null when it does not and the
     * executor reads $source. Otherwise the launch: its rows are identical to the launched steps run over the source's
     * own scan of $criteria, with the source's rows counted into $sources.
     */
    public function read(
        PhysicalPlan $plan,
        SplittableExtractor $source,
        Criteria $criteria,
        FlowContext $context,
        ?SourceCounts $sources = null,
    ): ?Launch;
}
