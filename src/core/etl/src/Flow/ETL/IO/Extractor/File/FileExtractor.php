<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Refreshable;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Path;

/**
 * A source that lists files under a path. Its schema is a property of the source - derived from the full
 * listing - and a pruned read does not change it.
 */
interface FileExtractor extends Extractor, Refreshable
{
    public function source(): Path;

    /**
     * The partition block of this source's schema and nothing else - typed exactly as the read emits it,
     * empty when the source declares no partition columns. The planner asks it to decide whether a predicate
     * can be evaluated from the path alone.
     */
    public function partitionSchema(): Schema;

    /**
     * True when this source skips data with $conjunct before it decodes rows. The planner pushes a conjunct over
     * the source's columns only when this is true; a conjunct over partition columns only is pushed into every source.
     */
    public function prunes(ScalarFunction $conjunct): bool;
}
