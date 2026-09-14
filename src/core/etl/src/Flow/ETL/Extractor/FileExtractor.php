<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Schema;
use Flow\Filesystem\Path;

/**
 * A source that lists files under a path: it honours Scan::$pathFilter when it reads. Its schema is a
 * property of the source - derived from the full listing - and a pruned read does not change it.
 */
interface FileExtractor extends Scannable
{
    public function source(): Path;

    /**
     * The partition block of this source's schema and nothing else - typed exactly as the read emits it,
     * empty when the source declares no partition columns. The planner asks it to decide whether a predicate
     * can be evaluated from the path alone.
     */
    public function partitionSchema(): Schema;
}
