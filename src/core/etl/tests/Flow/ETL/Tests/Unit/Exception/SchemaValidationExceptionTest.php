<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Exception;

use Flow\ETL\Exception\SchemaValidationException;
use Flow\ETL\Tests\FlowTestCase;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\string_schema;

final class SchemaValidationExceptionTest extends FlowTestCase
{
    public function test_diff_message() : void
    {
        $expected = schema(
            integer_schema('id'),
            string_schema('name'),
            string_schema('email'),
        );

        $given = schema(
            integer_schema('id'),
            string_schema('name'),
            string_schema('email', true),
            string_schema('address'),
        );

        $exception = new SchemaValidationException($expected, $given);

        self::assertStringContainsString(
<<<EXCEPTION
Schema validation failed: 
  Mismatched Definitions: 
    |-- expected: email<string>, given: email<?string>
  Unexpected Definitions: 
    |-- address<string>
EXCEPTION,
            $exception->getMessage()
        );
    }
}