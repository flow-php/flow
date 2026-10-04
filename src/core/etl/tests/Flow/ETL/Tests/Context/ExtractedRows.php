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
        $context ??= flow_context();
        $extracted = null;

        foreach (FlowTestCase::extracted($extractor, $context, $limit, $pathFilter) as $batch) {
            $extracted = $extracted === null ? $batch : $extracted->concat($context->backend(), $batch);
        }

        return $extracted ?? rows(schema());
    }

    /**
     * Two reads of one extractor, their batches pulled in turn - each read's rows.
     *
     * @return array{Rows, Rows}
     */
    public static function interleaved(Extractor $extractor, ?FlowContext $context = null): array
    {
        $context ??= flow_context();
        $reads = [$extractor->extract($context), $extractor->extract($context)];
        /** @var array{0: null|Rows, 1: null|Rows} $extracted */
        $extracted = [null, null];

        while ($reads[0]->valid() || $reads[1]->valid()) {
            foreach ($reads as $i => $read) {
                if ($read->valid()) {
                    $extracted[$i] = $extracted[$i] === null
                        ? $read->current()
                        : $extracted[$i]->concat($context->backend(), $read->current());
                    $read->next();
                }
            }
        }

        return [$extracted[0] ?? rows(schema()), $extracted[1] ?? rows(schema())];
    }
}
