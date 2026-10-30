<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation\Exception;

use Flow\ETL\Foundation\Exception\DataDependentSchemaException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Operators\Transformer\JoinEachRowsTransformer;
use Flow\ETL\Tests\FlowTestCase;

final class DataDependentSchemaExceptionTest extends FlowTestCase
{
    public function test_it_belongs_to_the_schema_not_derivable_family(): void
    {
        static::assertInstanceOf(SchemaNotDerivableException::class, DataDependentSchemaException::step(
            JoinEachRowsTransformer::class,
            'a reason',
        ));
    }

    public function test_step_names_the_step_and_the_reason(): void
    {
        static::assertSame(
            'Flow\ETL\Operators\Transformer\JoinEachRowsTransformer cannot describe its output before rows flow: '
            . "its right side is built from each left batch's row values.",
            DataDependentSchemaException::step(
                JoinEachRowsTransformer::class,
                "its right side is built from each left batch's row values",
            )->getMessage(),
        );
    }
}
