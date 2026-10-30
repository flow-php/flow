<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor;

use Flow\ETL\IO\Extractor;
use Flow\ETL\Values\Rows;

interface ExtractorFactory
{
    /**
     * The source of the right rows for one left batch.
     */
    public function from(Rows $rows): Extractor;
}
