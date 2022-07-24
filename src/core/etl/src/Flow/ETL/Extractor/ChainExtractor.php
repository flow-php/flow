<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;

/**
 * @psalm-immutable
 */
final class ChainExtractor implements Extractor
{
    /**
     * @var array<Extractor>
     */
    private readonly array $extractors;

    public function __construct(Extractor ...$extractors)
    {
        $this->extractors = $extractors;
    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($this->extractors as $extractor) {
            foreach ($extractor->extract($context) as $rows) {
                yield $rows;
            }
        }
    }
}
