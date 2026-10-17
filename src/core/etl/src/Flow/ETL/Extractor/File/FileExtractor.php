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
     * Reads what the plan pushed into this read. Every field of $scan is a hint: the plan still enforces the
     * limit, lists through the path filter and evaluates the predicate, so a source that reads or lists more, or
     * ignores a field, is still correct. extract() is scan() with only a limit.
     *
     * @param FileScan $scan $scan->limit (see Extractor::extract()), $scan->pathFilter (what this read lists) and
     *                       $scan->filter (a predicate over the source's columns)
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
