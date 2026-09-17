<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\SideInput;
use SplObjectStorage;

use function array_pop;
use function array_push;

final readonly class Repeatability
{
    public function of(Extractor $extractor): bool
    {
        if ($extractor instanceof NestedPlan) {
            return $this->ofPlan($extractor->plan()->logical);
        }

        if ($extractor instanceof OverridingExtractor) {
            foreach ($extractor->extractors() as $wrapped) {
                if (!$this->of($wrapped)) {
                    return false;
                }
            }
        }

        return $extractor instanceof RewindableExtractor && $extractor->isRepeatable();
    }

    public function ofPlan(LogicalPlan $plan): bool
    {
        /** @var SplObjectStorage<Node, null> $seen */
        $seen = new SplObjectStorage();
        $stack = [$plan->root];

        while ($stack !== []) {
            $node = array_pop($stack);

            if ($seen->offsetExists($node)) {
                continue;
            }

            $seen[$node] = null;

            if ($node instanceof Read && !$this->of($node->extractor())) {
                return false;
            }

            if ($node instanceof SideInput && !$this->ofPlan($node->plan()->logical)) {
                return false;
            }

            array_push($stack, ...$node->children());
        }

        return true;
    }
}
