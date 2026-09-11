<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class SanitizeTest extends FlowTestCase
{
    public function test_sanitize_on_non_string_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer".');

        ref('value')->sanitize()->eval(row(['value' => 1000]), flow_context());
    }

    public function test_sanitize_on_valid_string(): void
    {
        static::assertSame('****', ref('value')->sanitize()->eval(row(['value' => 'test']), flow_context()));
    }

    public function test_sanitize_on_valid_string_with_left_characters(): void
    {
        static::assertSame('te**', ref('value')
            ->sanitize(skipCharacters: lit(2))
            ->eval(row(['value' => 'test']), flow_context()));
    }

    public function test_sanitize_on_valid_string_with_left_characters_longer_than_string(): void
    {
        static::assertSame('****', ref('value')
            ->sanitize(skipCharacters: lit(5))
            ->eval(row(['value' => 'test']), flow_context()));
    }

    public function test_sanitize_on_valid_string_with_placeholder(): void
    {
        static::assertSame('----', ref('value')->sanitize(lit('-'))->eval(row(['value' => 'test']), flow_context()));
    }
}
