<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class ExtractedRows
{
    public static function of(Extractor $extractor, ?FlowContext $context = null): Rows
    {
        $extracted = rows(schema());

        foreach ($extractor->extract($context ?? flow_context()) as $batch) {
            $extracted = $extracted->merge($batch);
        }

        return $extracted;
    }
}
