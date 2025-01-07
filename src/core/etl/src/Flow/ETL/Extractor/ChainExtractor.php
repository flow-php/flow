<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\{Extractor, FlowContext};

final readonly class ChainExtractor implements Extractor, OverridingExtractor
{
    /**
     * @var array<Extractor>
     */
    private array $extractors;

    public function __construct(Extractor ...$extractors)
    {
        $this->extractors = $extractors;
    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($this->extractors as $extractor) {
            foreach ($extractor->extract($context) as $rows) {
                $signal = yield $rows;

                if ($signal === Signal::STOP) {
                    return;
                }
            }
        }
    }

    public function extractors() : array
    {
        return $this->extractors;
    }
}
