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

final class StringTitleTest extends FlowTestCase
{
    public function test_string_title(): void
    {
        static::assertSame('Foo ijssel', ref('str')
            ->stringTitle()
            ->eval(array_to_row(['str' => 'foo ijssel'], schema(str_schema('str'))), flow_context()));
    }

    public function test_string_title_allwords(): void
    {
        static::assertSame('Foo Ijssel', ref('str')
            ->stringTitle(allWords: true)
            ->eval(array_to_row(['str' => 'foo ijssel'], schema(str_schema('str'))), flow_context()));
    }

    public function test_string_title_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringTitle function requires non-null value');

        ref('str')
            ->stringTitle()
            ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context());
    }
}
