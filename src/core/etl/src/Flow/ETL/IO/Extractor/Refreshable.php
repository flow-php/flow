<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor;

interface Refreshable
{
    /**
     * Drops the listing and everything derived from it, so the next run lists the source again. A run calls it when
     * it ends; a plan that does not run (explain(), schema()) keeps what it listed for the run that follows.
     */
    public function refresh(): void;
}
