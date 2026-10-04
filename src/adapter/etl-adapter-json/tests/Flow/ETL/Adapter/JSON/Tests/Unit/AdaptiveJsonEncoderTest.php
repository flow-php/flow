<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Adapter\JSON\AdaptiveJsonEncoder;
use Flow\ETL\Adapter\JSON\JsonFraming;
use Flow\ETL\Adapter\JSON\JsonOpenSink;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowPhpExtension;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Stream\StringDestinationStream;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;

use const JSON_PRETTY_PRINT;
use const JSON_THROW_ON_ERROR;
use const JSON_UNESCAPED_SLASHES;

final class AdaptiveJsonEncoderTest extends FlowTestCase
{
    public function test_a_sink_writes_with_the_options_of_its_encoder(): void
    {
        $stream = new StringDestinationStream(path('memory://out.json'));
        $sink = new JsonOpenSink(
            $stream,
            new AdaptiveJsonEncoder(JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES, 'd/m/Y H:i', 'Y/m/d'),
            JsonFraming::Array,
        );

        $sink->write(array_to_rows(
            [['at' => new DateTimeImmutable('2023-10-01 12:02:01 UTC'), 'on' => new DateTimeImmutable('2023-10-01')]],
            schema(datetime_schema('at'), date_schema('on')),
        ));
        $sink->close();

        static::assertSame('[{"at":"01/10/2023 12:02","on":"2023/10/01"}]', $stream->content());
    }

    public function test_a_flag_the_native_writer_does_not_render_is_written_through_php(): void
    {
        $stream = new StringDestinationStream(path('memory://out.json'));
        $sink = new JsonOpenSink(
            $stream,
            new AdaptiveJsonEncoder(JSON_PRETTY_PRINT, 'Y-m-d', 'Y-m-d'),
            JsonFraming::Lines,
        );

        $sink->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $sink->close();

        static::assertSame("{\n    \"id\": 1\n}\n", $stream->content());
    }

    public function test_a_flow_php_of_another_abi_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('does not match flow-php/etl');

        new AdaptiveJsonEncoder(extension: new FlowPhpExtension(true, FlowPhpExtension::ABI + 1, '0.46.0'));
    }
}
