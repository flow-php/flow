<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class StringMatchTest extends FlowTestCase
{
    public function test_no_matches_found(): void
    {
        $result = ref('str')->stringMatch('/foo/')->eval(row(['str' => 'hello world']), flow_context());

        static::assertNull($result);
    }

    public function test_null_haystack(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringMatch function requires non-null haystack');

        $result = ref('str')->stringMatch('/hello/')->eval(row(['str' => null]), flow_context());

        static::assertNull($result);
    }

    public function test_null_pattern(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringMatch function requires non-null pattern');

        $result = ref('str')
            ->stringMatch(ref('pattern'))
            ->eval(row(['str' => 'hello world', 'pattern' => null]), flow_context());

        static::assertNull($result);
    }

    public function test_successful_pattern_match(): void
    {
        $result = ref('str')->stringMatch('/hello/')->eval(row(['str' => 'hello world']), flow_context());

        static::assertEquals(['hello'], $result);
    }

    public function test_with_scalar_function_parameter(): void
    {
        $result = ref('str')
            ->stringMatch(ref('pattern'))
            ->eval(row(['str' => 'hello world', 'pattern' => '/world/']), flow_context());

        static::assertEquals(['world'], $result);
    }
}
