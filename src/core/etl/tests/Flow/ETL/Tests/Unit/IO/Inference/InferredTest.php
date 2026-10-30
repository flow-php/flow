<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Inference;

use Flow\ETL\IO\Inference\Inferred;
use Flow\ETL\IO\Inference\SampledUnits;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class InferredTest extends FlowTestCase
{
    public function test_it_carries_the_schema_and_the_units(): void
    {
        $inferred = new Inferred(schema(int_schema('id')), new SampledUnits([]));

        static::assertEquals(schema(int_schema('id')), $inferred->schema);
        static::assertSame([], $inferred->units->samples);
    }
}
