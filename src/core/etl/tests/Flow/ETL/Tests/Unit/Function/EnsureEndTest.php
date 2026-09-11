<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class EnsureEndTest extends FlowTestCase
{
    public function test_empty_string_with_suffix(): void
    {
        $result = ref('str')->ensureEnd('_suffix')->eval(row(['str' => '']), flow_context());

        static::assertEquals('_suffix', $result);
    }

    public function test_null_suffix(): void
    {
        $result = ref('str')->ensureEnd(ref('suffix'))->eval(row(['str' => 'hello', 'suffix' => null]), flow_context());

        static::assertEquals('hello', $result);
    }

    public function test_null_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('EnsureEnd function requires non-null value');

        $result = ref('str')->ensureEnd('_suffix')->eval(row(['str' => null]), flow_context());

        static::assertNull($result);
    }

    public function test_string_already_ends_with_suffix(): void
    {
        $result = ref('str')->ensureEnd('.txt')->eval(row(['str' => 'document.txt']), flow_context());

        static::assertEquals('document.txt', $result);
    }

    public function test_string_doesnt_end_with_suffix(): void
    {
        $result = ref('str')->ensureEnd('.txt')->eval(row(['str' => 'document']), flow_context());

        static::assertEquals('document.txt', $result);
    }

    public function test_string_with_empty_suffix(): void
    {
        $result = ref('str')->ensureEnd('')->eval(row(['str' => 'hello']), flow_context());

        static::assertEquals('hello', $result);
    }

    public function test_with_scalar_function_parameter(): void
    {
        $result = ref('str')
            ->ensureEnd(ref('suffix'))
            ->eval(row(['str' => 'document', 'suffix' => '.pdf']), flow_context());

        static::assertEquals('document.pdf', $result);
    }
}
