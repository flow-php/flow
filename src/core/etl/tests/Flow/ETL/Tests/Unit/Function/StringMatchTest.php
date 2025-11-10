<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{flow_context, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class StringMatchTest extends FlowTestCase
{
    public function test_no_matches_found() : void
    {
        $result = ref('str')->stringMatch('/foo/')->eval(
            row(str_entry('str', 'hello world')),
            flow_context()
        );

        self::assertNull($result);
    }

    public function test_null_haystack() : void
    {
        $result = ref('str')->stringMatch('/hello/')->eval(
            row(str_entry('str', null)),
            flow_context()
        );

        self::assertNull($result);
    }

    public function test_null_pattern() : void
    {
        $result = ref('str')->stringMatch(ref('pattern'))->eval(
            row(
                str_entry('str', 'hello world'),
                str_entry('pattern', null)
            ),
            flow_context()
        );

        self::assertNull($result);
    }

    public function test_successful_pattern_match() : void
    {
        $result = ref('str')->stringMatch('/hello/')->eval(
            row(str_entry('str', 'hello world')),
            flow_context()
        );

        self::assertEquals(['hello'], $result);
    }

    public function test_with_scalar_function_parameter() : void
    {
        $result = ref('str')->stringMatch(ref('pattern'))->eval(
            row(
                str_entry('str', 'hello world'),
                str_entry('pattern', '/world/')
            ),
            flow_context()
        );

        self::assertEquals(['world'], $result);
    }
}
