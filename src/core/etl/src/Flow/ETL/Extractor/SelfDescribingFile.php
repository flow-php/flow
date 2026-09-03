<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Schema;

interface SelfDescribingFile
{
    public function close(): void;

    public function schema(): Schema;

    public function source(): SourceFile;
}
