<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Formatter\ASCII;

use Flow\ETL\Formatter\ASCII\ASCIIHeaders;
use Flow\ETL\Formatter\ASCII\Body;
use Flow\ETL\Formatter\ASCII\Headers;
use Flow\ETL\Tests\CommandOutputNormalizer;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class ASCIIHeadersTest extends FlowTestCase
{
    use CommandOutputNormalizer;

    public function test_printing_ascii_headers(): void
    {
        $rows = rows(
            row(int_entry('id', 1), float_entry('value', 1.4)),
            row(int_entry('id', 2), float_entry('value', 3.4)),
        );

        $headers = new ASCIIHeaders(new Headers($rows), new Body($rows));

        self::assertCommandOutputContains(<<<'TABLE'
            +----+----------+
            | id |    value |
            +----+----------+
            TABLE, $headers->print(false));
    }
}
