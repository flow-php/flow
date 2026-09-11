<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;

final readonly class Repeatability
{
    public function of(Extractor $extractor): bool
    {
        if ($extractor instanceof OverridingExtractor) {
            foreach ($extractor->extractors() as $wrapped) {
                if (!$this->of($wrapped)) {
                    return false;
                }
            }
        }

        return $extractor instanceof RewindableExtractor && $extractor->isRepeatable();
    }
}
