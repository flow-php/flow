<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class StringMatchAllTest extends FlowTestCase
{
    public function test_empty_haystack_string(): void
    {
        $result = ref('str')->stringMatchAll('/hello/')->eval(row(str_entry('str', '')), flow_context());

        static::assertEquals([], $result);
    }

    public function test_multiple_successful_pattern_matches(): void
    {
        $result = ref('str')
            ->stringMatchAll('/\d+/')
            ->eval(row(str_entry('str', 'test 123 and 456 and 789')), flow_context());

        static::assertEquals([['123'], ['456'], ['789']], $result);
    }

    public function test_no_matches_found(): void
    {
        $result = ref('str')->stringMatchAll('/foo/')->eval(row(str_entry('str', 'hello world')), flow_context());

        static::assertEquals([], $result);
    }

    public function test_null_haystack(): void
    {
        $result = ref('str')->stringMatchAll('/hello/')->eval(row(str_entry('str', null)), flow_context());

        static::assertNull($result);
    }

    public function test_null_pattern(): void
    {
        $result = ref('str')
            ->stringMatchAll(ref('pattern'))
            ->eval(row(str_entry('str', 'hello world'), str_entry('pattern', null)), flow_context());

        static::assertEquals([], $result);
    }

    public function test_with_scalar_function_parameter(): void
    {
        $result = ref('str')
            ->stringMatchAll(ref('pattern'))
            ->eval(row(str_entry('str', 'test 123 and 456'), str_entry('pattern', '/\d+/')), flow_context());

        static::assertEquals([['123'], ['456']], $result);
    }
}
