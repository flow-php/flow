<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use function Flow\ETL\DSL\rows;
use Flow\ETL\{Extractor, FlowContext};

final class EmptyExtractor implements Extractor
{
    public function extract(FlowContext $context) : \Generator
    {
        yield rows();
    }
}
