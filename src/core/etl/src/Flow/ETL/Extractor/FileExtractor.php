<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Generator;

/**
 * A source that lists files under a path. Its schema is a property of the source - derived from the full
 * listing - and a pruned read does not change it.
 */
interface FileExtractor extends Extractor
{
    /**
     * @param null|int<1, max> $limit see Extractor::extract()
     * @param Filter $pathFilter what this read lists. The Filter step stays in the plan, so a source that lists
     *                           more, or ignores it, is still correct.
     *
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context, ?int $limit = null, Filter $pathFilter = new OnlyFiles()): Generator;

    public function source(): Path;

    /**
     * The partition block of this source's schema and nothing else - typed exactly as the read emits it,
     * empty when the source declares no partition columns. The planner asks it to decide whether a predicate
     * can be evaluated from the path alone.
     */
    public function partitionSchema(): Schema;
}
