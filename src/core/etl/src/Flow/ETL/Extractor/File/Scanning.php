<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\File;

use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Generator;

/**
 * @require-implements FileExtractor
 */
trait Scanning
{
    /**
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function extract(FlowContext $context, ?int $limit = null): Generator
    {
        return $this->scan($context, new FileScan(limit: $limit));
    }
}
