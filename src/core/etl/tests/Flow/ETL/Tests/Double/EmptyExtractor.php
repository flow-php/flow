<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;

final class EmptyExtractor implements Extractor
{
    public function extract(FlowContext $context) : \Generator
    {
        yield new Rows();
    }
}
