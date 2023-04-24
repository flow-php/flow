<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Formatter\ASCII;

use Flow\ETL\DSL\Entry;
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
            Row::create(Entry::integer('id', 1), Entry::float('value', 1.4)),
            Row::create(Entry::integer('id', 2), Entry::float('value', 3.4))
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
