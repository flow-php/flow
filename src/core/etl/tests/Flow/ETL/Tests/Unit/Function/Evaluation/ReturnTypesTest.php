<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function\Evaluation;

use Flow\ETL\Column\ValueColumn;
use Flow\ETL\Function\Evaluation\ReturnTypes;
use Flow\ETL\Tests\Double\ReturnsSpyFunction;
use Flow\ETL\Tests\FlowTestCase;

use function array_fill;
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class ReturnTypesTest extends FlowTestCase
{
    public function test_a_node_derives_its_type_once_across_batches(): void
    {
        $function = new ReturnsSpyFunction();

        foreach ([[['id' => 1]], [['id' => 2], ['id' => 3]], [['id' => 4]]] as $batch) {
            static::assertSame(
                array_fill(0, count($batch), 1),
                $function->eval(array_to_rows($batch, schema(int_schema('id'))), flow_context())->values(),
            );
        }

        static::assertSame(1, $function->derivations);
    }

    public function test_a_type_that_cannot_be_derived_is_tried_once(): void
    {
        $function = new ReturnsSpyFunction(derivable: false);

        foreach ([[['id' => 1]], [['id' => 2]]] as $batch) {
            static::assertInstanceOf(ValueColumn::class, $function->eval(
                array_to_rows($batch, schema(int_schema('id'))),
                flow_context(),
            ));
        }

        static::assertNull(ReturnTypes::of($function));
        static::assertSame(1, $function->derivations);
    }

    public function test_every_node_derives_its_own_type(): void
    {
        $first = new ReturnsSpyFunction();
        $second = new ReturnsSpyFunction();

        static::assertSame('integer', ReturnTypes::of($first)?->toString());
        static::assertSame('integer', ReturnTypes::of($second)?->toString());
        static::assertSame(1, $first->derivations);
        static::assertSame(1, $second->derivations);
    }
}
