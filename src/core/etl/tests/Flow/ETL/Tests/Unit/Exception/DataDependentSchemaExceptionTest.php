<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Exception;

use Flow\ETL\Exception\DataDependentSchemaException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\JoinEachRowsTransformer;

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
            'Flow\ETL\Transformer\JoinEachRowsTransformer cannot describe its output before rows flow: '
            . "its right side is built from each left batch's row values.",
            DataDependentSchemaException::step(
                JoinEachRowsTransformer::class,
                "its right side is built from each left batch's row values",
            )->getMessage(),
        );
    }
}
