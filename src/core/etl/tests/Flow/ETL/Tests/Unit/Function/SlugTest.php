<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class SlugTest extends FlowTestCase
{
    public function test_a_malformed_separator_operand_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ref('value')->slug(lit(123))->eval(row(str_entry('value', 'hello world')), flow_context());
    }

    public function test_ascii_on_null(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Slug function requires non-null value');

        ref('str')->slug()->eval(row(str_entry('str', null)), flow_context());
    }

    public function test_slug(): void
    {
        static::assertSame('azcz', ref('str')->slug()->eval(row(str_entry('str', 'ąźćż')), flow_context()));
    }

    public function test_slug_separator(): void
    {
        static::assertSame('Some_Text', ref('str')
            ->slug('_')
            ->eval(row(str_entry('str', 'Some Text')), flow_context()));
    }
}
