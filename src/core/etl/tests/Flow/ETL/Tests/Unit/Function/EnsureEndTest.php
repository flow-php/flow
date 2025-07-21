<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class EnsureEndTest extends FlowTestCase
{
    public function test_empty_string_with_suffix() : void
    {
        $result = ref('str')->ensureEnd('_suffix')->eval(
            row(str_entry('str', ''))
        );

        self::assertEquals('_suffix', $result);
    }

    public function test_null_suffix() : void
    {
        $result = ref('str')->ensureEnd(ref('suffix'))->eval(
            row(
                str_entry('str', 'hello'),
                str_entry('suffix', null)
            )
        );

        self::assertEquals('hello', $result);
    }

    public function test_null_value() : void
    {
        $result = ref('str')->ensureEnd('_suffix')->eval(
            row(str_entry('str', null))
        );

        self::assertNull($result);
    }

    public function test_string_already_ends_with_suffix() : void
    {
        $result = ref('str')->ensureEnd('.txt')->eval(
            row(str_entry('str', 'document.txt'))
        );

        self::assertEquals('document.txt', $result);
    }

    public function test_string_doesnt_end_with_suffix() : void
    {
        $result = ref('str')->ensureEnd('.txt')->eval(
            row(str_entry('str', 'document'))
        );

        self::assertEquals('document.txt', $result);
    }

    public function test_string_with_empty_suffix() : void
    {
        $result = ref('str')->ensureEnd('')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('hello', $result);
    }

    public function test_with_scalar_function_parameter() : void
    {
        $result = ref('str')->ensureEnd(ref('suffix'))->eval(
            row(
                str_entry('str', 'document'),
                str_entry('suffix', '.pdf')
            )
        );

        self::assertEquals('document.pdf', $result);
    }
}
