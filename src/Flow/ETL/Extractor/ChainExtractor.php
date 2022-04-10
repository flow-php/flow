<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;

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

    public function extract() : \Generator
    {
        foreach ($this->extractors as $extractor) {
            foreach ($extractor->extract() as $rows) {
                yield $rows;
            }
        }
    }
}
