<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class StringMatchAllTest extends FlowTestCase
{
    public function test_empty_haystack_string(): void
    {
        $result = ref('str')->stringMatchAll('/hello/')->eval(row(['str' => '']), flow_context());

        static::assertEquals([], $result);
    }

    public function test_multiple_successful_pattern_matches(): void
    {
        $result = ref('str')->stringMatchAll('/\d+/')->eval(row(['str' => 'test 123 and 456 and 789']), flow_context());

        static::assertEquals([['123'], ['456'], ['789']], $result);
    }

    public function test_no_matches_found(): void
    {
        $result = ref('str')->stringMatchAll('/foo/')->eval(row(['str' => 'hello world']), flow_context());

        static::assertEquals([], $result);
    }

    public function test_null_haystack(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringMatchAll function requires non-null haystack');

        ref('str')->stringMatchAll('/hello/')->eval(row(['str' => null]), flow_context());
    }

    public function test_null_pattern(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringMatchAll function requires non-null pattern');

        ref('str')
            ->stringMatchAll(ref('pattern'))
            ->eval(row(['str' => 'hello world', 'pattern' => null]), flow_context());
    }

    public function test_with_scalar_function_parameter(): void
    {
        $result = ref('str')
            ->stringMatchAll(ref('pattern'))
            ->eval(row(['str' => 'test 123 and 456', 'pattern' => '/\d+/']), flow_context());

        static::assertEquals([['123'], ['456']], $result);
    }
}
