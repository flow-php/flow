<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;

final class CollectingExtractor implements Extractor, OverridingExtractor
{
    public function __construct(private readonly Extractor $extractor)
    {
    }

    public function extract(FlowContext $context) : \Generator
    {
        $collectedRows = new Rows();

        foreach ($this->extractor->extract($context) as $rows) {
            $collectedRows = $collectedRows->merge($rows);
        }

        yield $collectedRows;
    }

    public function extractors() : array
    {
        return [$this->extractor];
    }
}
