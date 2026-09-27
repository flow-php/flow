<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class SlugTest extends FlowTestCase
{
    public function test_a_malformed_separator_operand_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ref('value')
            ->slug(lit(123))
            ->eval(array_to_row(['value' => 'hello world'], schema(str_schema('value'))), flow_context());
    }

    public function test_ascii_on_null(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Slug function requires non-null value');

        ref('str')
            ->slug()
            ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context());
    }

    public function test_slug(): void
    {
        static::assertSame('azcz', ref('str')
            ->slug()
            ->eval(array_to_row(['str' => 'ąźćż'], schema(str_schema('str'))), flow_context()));
    }

    public function test_slug_separator(): void
    {
        static::assertSame('Some_Text', ref('str')
            ->slug('_')
            ->eval(array_to_row(['str' => 'Some Text'], schema(str_schema('str'))), flow_context()));
    }
}
