<?php declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Row\Sort;

/**
 * @internal
 */
interface ExternalSort
{
    public function sortBy(Sort ...$entries) : Extractor;
}
