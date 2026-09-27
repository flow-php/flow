<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Formatter\ASCII;

use Flow\ETL\Formatter\ASCII\ASCIIHeaders;
use Flow\ETL\Formatter\ASCII\Body;
use Flow\ETL\Formatter\ASCII\Headers;
use Flow\ETL\Tests\CommandOutputNormalizer;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class ASCIIHeadersTest extends FlowTestCase
{
    use CommandOutputNormalizer;

    public function test_printing_ascii_headers(): void
    {
        $rows = array_to_rows(
            [['id' => 1, 'value' => 1.4], ['id' => 2, 'value' => 3.4]],
            schema(int_schema('id'), float_schema('value')),
        );

        $headers = new ASCIIHeaders(new Headers($rows), new Body($rows));

        self::assertCommandOutputContains(<<<'TABLE'
            +----+----------+
            | id |    value |
            +----+----------+
            TABLE, $headers->print(false));
    }
}
