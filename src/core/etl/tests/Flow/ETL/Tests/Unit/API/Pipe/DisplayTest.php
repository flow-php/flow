<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Pipe;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\display;
use function Flow\ETL\DSL\from_array;

final class DisplayTest extends FlowTestCase
{
    public function test_display_renders_the_rows(): void
    {
        static::assertSame('+---+
| a |
+---+
| 1 |
| 2 |
+---+
2 rows
', display()(df()->read(from_array([['a' => 1], ['a' => 2]]))));
    }
}
