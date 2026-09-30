<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVOpenSink;
use Flow\ETL\Adapter\CSV\Tests\Mother\CSVEncoderMother;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Stream\StringDestinationStream;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;

final class CSVOpenSinkTest extends FlowTestCase
{
    public function test_the_header_is_written_once_before_the_first_batch(): void
    {
        $stream = new StringDestinationStream(path('memory://out.csv'));
        $sink = new CSVOpenSink($stream, CSVEncoderMother::php(), true);

        $sink->write(array_to_rows(
            [['id' => 1, 'first name' => 'a']],
            schema(int_schema('id'), str_schema('first name')),
        ));
        $sink->write(array_to_rows(
            [['id' => 2, 'first name' => 'b']],
            schema(int_schema('id'), str_schema('first name')),
        ));

        static::assertSame("id,\"first name\"\n1,a\n2,b\n", $stream->content());
    }

    public function test_no_header_when_it_is_disabled(): void
    {
        $stream = new StringDestinationStream(path('memory://out.csv'));
        $sink = new CSVOpenSink($stream, CSVEncoderMother::php(), false);

        $sink->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $sink->write(array_to_rows([['id' => 2]], schema(int_schema('id'))));

        static::assertSame("1\n2\n", $stream->content());
    }
}
