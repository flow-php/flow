<?php declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Row\Reference;

/**
 * @internal
 */
interface ExternalSort
{
    public function sortBy(Reference ...$refs) : Extractor;
}
