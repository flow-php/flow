<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class ExtractedRows
{
    /**
     * @param null|int<1, max> $limit
     */
    public static function of(
        Extractor $extractor,
        ?FlowContext $context = null,
        ?int $limit = null,
        Filter $pathFilter = new OnlyFiles(),
    ): Rows {
        $extracted = rows(schema());

        foreach (FlowTestCase::extracted($extractor, $context ?? flow_context(), $limit, $pathFilter) as $batch) {
            $extracted = $extracted->merge($batch);
        }

        return $extracted;
    }
}
