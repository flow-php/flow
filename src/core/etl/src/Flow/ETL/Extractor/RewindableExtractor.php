<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;

/**
 * A rewindable extractor can be read more than once and yield the same rows each time, so a build-time
 * pass may scan it before extraction without consuming it.
 */
interface RewindableExtractor extends Extractor
{
    public function isRepeatable(): bool;
}
