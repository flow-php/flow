<?php declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Row\EntryReference;

/**
 * @internal
 */
interface ExternalSort
{
    public function sortBy(EntryReference ...$refs) : Extractor;
}
