<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class StringFoldTest extends FlowTestCase
{
    public function test_string_folded(): void
    {
        static::assertSame("die o'brian strasse", ref('str')
            ->stringFold()
            ->eval(row(['str' => "Die O'Brian Straße"]), flow_context()));
    }

    public function test_string_fold_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringFold function requires non-null value');

        ref('str')->stringFold()->eval(row(['str' => null]), flow_context());
    }
}
