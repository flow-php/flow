<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor;

/**
 * Plain data that crosses a process boundary; source order is its position in splits().
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
