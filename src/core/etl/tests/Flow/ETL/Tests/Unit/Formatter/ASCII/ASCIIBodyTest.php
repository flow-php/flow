<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Formatter\ASCII;

use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use Flow\ETL\Formatter\ASCII\ASCIIBody;
use Flow\ETL\Formatter\ASCII\Body;
use Flow\ETL\Formatter\ASCII\Headers;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class ASCIIBodyTest extends TestCase
{
    public function test_printing_ascii_body() : void
    {
        $rows = new Rows(
            Row::create(int_entry('id', 1), float_entry('value', 1.4)),
            Row::create(int_entry('id', 2), float_entry('value', 3.4))
        );

        $headers = new ASCIIBody(
            new Headers($rows),
            new Body($rows)
        );

        $this->assertStringContainsString(
            <<<'TABLE'
|  1 |   1.4 |
|  2 |   3.4 |
+----+-------+
TABLE,
            $headers->print(false)
        );
    }
}
