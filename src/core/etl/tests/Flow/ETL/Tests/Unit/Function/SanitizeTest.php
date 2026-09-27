<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class SanitizeTest extends FlowTestCase
{
    public function test_sanitize_on_non_string_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer".');

        ref('value')->sanitize()->eval(array_to_row(['value' => 1000], schema(int_schema('value'))), flow_context());
    }

    public function test_sanitize_on_valid_string(): void
    {
        static::assertSame('****', ref('value')
            ->sanitize()
            ->eval(array_to_row(['value' => 'test'], schema(str_schema('value'))), flow_context()));
    }

    public function test_sanitize_on_valid_string_with_left_characters(): void
    {
        static::assertSame('te**', ref('value')
            ->sanitize(skipCharacters: lit(2))
            ->eval(array_to_row(['value' => 'test'], schema(str_schema('value'))), flow_context()));
    }

    public function test_sanitize_on_valid_string_with_left_characters_longer_than_string(): void
    {
        static::assertSame('****', ref('value')
            ->sanitize(skipCharacters: lit(5))
            ->eval(array_to_row(['value' => 'test'], schema(str_schema('value'))), flow_context()));
    }

    public function test_sanitize_on_valid_string_with_placeholder(): void
    {
        static::assertSame('----', ref('value')
            ->sanitize(lit('-'))
            ->eval(array_to_row(['value' => 'test'], schema(str_schema('value'))), flow_context()));
    }
}
