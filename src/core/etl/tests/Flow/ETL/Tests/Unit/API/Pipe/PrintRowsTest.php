<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Pipe;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\print_rows;

final class PrintRowsTest extends FlowTestCase
{
    public function test_print_rows_prints_the_rows(): void
    {
        $this->expectOutputString('+---+
| a |
+---+
| 1 |
| 2 |
+---+
2 rows
');

        print_rows()(df()->read(from_array([['a' => 1], ['a' => 2]])));
    }
}
