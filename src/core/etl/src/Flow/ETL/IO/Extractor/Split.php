<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor;

/**
 * One piece of a source a worker reads on its own: plain data, small enough to cross a process boundary, complete
 * enough that the worker needs nothing else to know which rows are its. Source order is the position in the list
 * SplittableExtractor::splits() returned. A file source cuts byte windows (FileSplit); a database source would cut key
 * ranges; each kind is read through the scan of its kind.
 */
interface Split
{
    /**
     * The on-disk bytes this split covers - what a launcher sizes its workers by; 0 when the kind cannot know.
     *
     * @return int<0, max>
     */
    public function bytes(): int;
}
