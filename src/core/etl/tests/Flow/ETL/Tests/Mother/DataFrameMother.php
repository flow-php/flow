<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\API\DataFrame;
use Flow\ETL\IO\Extractor;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Logical\Writes;
use Flow\ETL\Runtime\FlowContext;

final class DataFrameMother
{
    public static function over(Extractor $extractor, FlowContext $context): DataFrame
    {
        return new DataFrame(new Read($extractor), new Writes(), $context);
    }
}
