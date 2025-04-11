<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema\Formatter;

use function Flow\ETL\DSL\schema;
use Flow\ETL\Schema\Formatter\JsonSchemaFormatter;
use Flow\ETL\Tests\FlowTestCase;

final class JsonSchemaFormatterTest extends FlowTestCase
{
    public function test_formatting_empty_schema() : void
    {
        self::assertEquals(
            '[]',
            (new JsonSchemaFormatter())->format(schema())
        );
    }
}
