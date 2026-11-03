<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Pipe;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\get_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class GetSchemaTest extends FlowTestCase
{
    public function test_get_schema_returns_the_frame_schema(): void
    {
        static::assertEquals(
            schema(int_schema('a', nullable: true)),
            get_schema()(df()->read(from_array([['a' => 1], ['a' => 2]]))),
        );
    }
}
