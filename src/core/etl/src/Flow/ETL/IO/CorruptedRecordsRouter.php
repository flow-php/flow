<?php

declare(strict_types=1);

namespace Flow\ETL\IO;

interface CorruptedRecordsRouter
{
    public function corruptedRecordsLoader(): ?Loader;

    /**
     * Corrupted records routed to $loader; null refuses them, the default.
     */
    public function withCorruptedRecords(?Loader $loader): static;
}
