<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Filesystem\Path;

interface FileExtractor
{
    public function source() : Path;
}
