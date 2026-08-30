<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class StringTitleTest extends FlowTestCase
{
    public function test_string_title(): void
    {
        static::assertSame('Foo ijssel', ref('str')->stringTitle()->eval(row(['str' => 'foo ijssel']), flow_context()));
    }

    public function test_string_title_allwords(): void
    {
        static::assertSame('Foo Ijssel', ref('str')
            ->stringTitle(allWords: true)
            ->eval(row(['str' => 'foo ijssel']), flow_context()));
    }

    public function test_string_title_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringTitle function requires non-null value');

        ref('str')->stringTitle()->eval(row(['str' => null]), flow_context());
    }
}
