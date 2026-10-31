<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Rows;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class ExtractedRows
{
    public static function of(
        Extractor $extractor,
        ?FlowContext $context = null,
        Criteria $criteria = new Criteria(),
    ): Rows {
        $context ??= flow_context();
        $extracted = null;

        foreach (FlowTestCase::extracted($extractor, $context->backend(), $criteria) as $batch) {
            $extracted = $extracted === null
                ? $batch
                : Rows::concat($extracted->schema(), $context->backend(), $extracted, $batch);
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
        $reads = [
            FlowTestCase::extracted($extractor, $context->backend(), new Criteria()),
            FlowTestCase::extracted($extractor, $context->backend(), new Criteria()),
        ];
        /** @var array{0: null|Rows, 1: null|Rows} $extracted */
        $extracted = [null, null];

        while ($reads[0]->valid() || $reads[1]->valid()) {
            foreach ($reads as $i => $read) {
                if ($read->valid()) {
                    $extracted[$i] = $extracted[$i] === null
                        ? $read->current()
                        : Rows::concat($extracted[$i]->schema(), $context->backend(), $extracted[$i], $read->current());
                    $read->next();
                }
            }
        }

        return [$extracted[0] ?? rows(schema()), $extracted[1] ?? rows(schema())];
    }
}
