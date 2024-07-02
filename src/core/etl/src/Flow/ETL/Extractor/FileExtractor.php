<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;

interface FileExtractor
{
    public function addFilter(Filter $filter) : void;

    public function filter() : Filter;

    public function source() : Path;
}
