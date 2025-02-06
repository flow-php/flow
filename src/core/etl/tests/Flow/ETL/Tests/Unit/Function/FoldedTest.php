<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{folded, lit};
use Flow\ETL\Tests\FlowTestCase;

final class FoldedTest extends FlowTestCase
{
    public function test_string_folded() : void
    {
        self::assertSame(
            "die o'brian strasse",
            folded(lit("Die O'Brian Straße"))->eval(row())
        );
    }
}
