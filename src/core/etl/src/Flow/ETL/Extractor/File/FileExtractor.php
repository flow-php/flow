<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\File;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Path;
use Generator;

/**
 * A source that lists files under a path. Its schema is a property of the source - derived from the full
 * listing - and a pruned read does not change it.
 */
interface FileExtractor extends Extractor
{
    /**
     * Reads what the plan pushed into this read and returns only rows matching $scan->filter. The limit and the
     * path filter are hints: the plan still enforces the limit and every path filter is part of $scan->filter, so a
     * source that reads or lists more is still correct. extract() is scan() with only a limit.
     *
     * @param FileScan $scan $scan->limit (see Extractor::extract()), $scan->pathFilter (what this read lists) and
     *                       $scan->filter (a predicate over the source's columns the returned rows match)
     *
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function scan(FlowContext $context, FileScan $scan): Generator;

    public function source(): Path;

    /**
     * The partition block of this source's schema and nothing else - typed exactly as the read emits it,
     * empty when the source declares no partition columns. The planner asks it to decide whether a predicate
     * can be evaluated from the path alone.
     */
    public function partitionSchema(): Schema;
}
