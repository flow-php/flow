<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Refreshable;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Path;

interface FileExtractor extends Extractor, Refreshable
{
    public function source(): Path;

    public function partitionSchema(): Schema;

    public function prunes(ScalarFunction $conjunct): bool;
}
