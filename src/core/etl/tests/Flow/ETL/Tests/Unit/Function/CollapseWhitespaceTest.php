<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class CollapseWhitespaceTest extends FlowTestCase
{
    public function test_empty_string(): void
    {
        $result = ref('str')
            ->collapseWhitespace()
            ->eval(array_to_row(['str' => ''], schema(str_schema('str'))), flow_context());

        static::assertEquals('', $result);
    }

    public function test_leading_and_trailing_whitespace(): void
    {
        $result = ref('str')
            ->collapseWhitespace()
            ->eval(array_to_row(['str' => '   Hello world   '], schema(str_schema('str'))), flow_context());

        static::assertEquals('Hello world', $result);
    }

    public function test_leading_whitespace(): void
    {
        $result = ref('str')
            ->collapseWhitespace()
            ->eval(array_to_row(['str' => '   Hello world'], schema(str_schema('str'))), flow_context());

        static::assertEquals('Hello world', $result);
    }

    public function test_mixed_whitespace_types(): void
    {
        $result = ref('str')
            ->collapseWhitespace()
            ->eval(array_to_row(['str' => "Hello\t\tworld\n\ntest"], schema(str_schema('str'))), flow_context());

        static::assertEquals('Hello world test', $result);
    }

    public function test_multiple_spaces_between_words(): void
    {
        $result = ref('str')
            ->collapseWhitespace()
            ->eval(array_to_row(['str' => 'Hello     world     test'], schema(str_schema('str'))), flow_context());

        static::assertEquals('Hello world test', $result);
    }

    public function test_null_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('CollapseWhitespace function requires non-null value');

        $result = ref('str')
            ->collapseWhitespace()
            ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context());

        static::assertNull($result);
    }

    public function test_single_spaces(): void
    {
        $result = ref('str')
            ->collapseWhitespace()
            ->eval(array_to_row(['str' => 'Hello world test'], schema(str_schema('str'))), flow_context());

        static::assertEquals('Hello world test', $result);
    }

    public function test_single_word(): void
    {
        $result = ref('str')
            ->collapseWhitespace()
            ->eval(array_to_row(['str' => 'Hello'], schema(str_schema('str'))), flow_context());

        static::assertEquals('Hello', $result);
    }

    public function test_trailing_whitespace(): void
    {
        $result = ref('str')
            ->collapseWhitespace()
            ->eval(array_to_row(['str' => 'Hello world   '], schema(str_schema('str'))), flow_context());

        static::assertEquals('Hello world', $result);
    }
}
