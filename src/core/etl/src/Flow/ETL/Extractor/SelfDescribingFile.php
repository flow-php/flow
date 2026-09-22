<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Schema;

interface SelfDescribingFile
{
    public function close(): void;

    public function schema(): Schema;

    public function source(): SourceFile;

    /**
     * What this one file's own metadata declares, read from what schema() already opened.
     */
    public function statistics(): Statistics;
}
