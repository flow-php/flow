<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\CallbackRowsTransformer;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class CallbackRowsTransformerTest extends FlowTestCase
{
    public function test_the_callback_receives_the_flow_context(): void
    {
        $seen = null;
        $context = flow_context();

        (new CallbackRowsTransformer(static function (Rows $rows, FlowContext $ctx) use (&$seen): Rows {
            $seen = $ctx;

            return $rows;
        }))->transform(rows(schema(int_schema('id')), row(['id' => 1])), $context);

        static::assertSame($context, $seen);
    }

    public function test_the_callback_reads_the_batch_schema_and_declares_the_new_one(): void
    {
        // the point of the batch-level callback: a lazy DataFrame cannot supply the schema up front,
        // so the callable derives the output shape from the batch it is handed
        static::assertEquals(
            rows(schema(int_schema('id'), bool_schema('odd')), row(['id' => 3, 'odd' => true])),
            (new CallbackRowsTransformer(static fn(Rows $rows, FlowContext $context): Rows => $rows->map(
                $rows->schema()->add(bool_schema('odd')),
                static fn($r) => row([...$r->values(), 'odd' => ((int) $r->get('id') % 2) === 1]),
            )))->transform(rows(schema(int_schema('id')), row(['id' => 3])), flow_context()),
        );
    }

    public function test_the_callback_returns_the_batch_untouched(): void
    {
        $batch = rows(schema(int_schema('id')), row(['id' => 1]));

        static::assertSame($batch, (new CallbackRowsTransformer(
            static fn(Rows $rows, FlowContext $context): Rows => $rows,
        ))->transform($batch, flow_context()));
    }
}
