<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Pipe;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\print_schema;

final class PrintSchemaTest extends FlowTestCase
{
    public function test_print_schema_prints_the_schema(): void
    {
        $this->expectOutputString("schema\n|-- a: ?integer\n");

        print_schema()(df()->read(from_array([['a' => 1], ['a' => 2]])));
    }
}
