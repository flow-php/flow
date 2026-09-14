<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Scan;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class ExtractedRows
{
    public static function of(Extractor $extractor, ?FlowContext $context = null, Scan $scan = new Scan()): Rows
    {
        $extracted = rows(schema());

        foreach (FlowTestCase::scanned($extractor, $context ?? flow_context(), $scan) as $batch) {
            $extracted = $extracted->merge($batch);
        }

        return $extracted;
    }
}
