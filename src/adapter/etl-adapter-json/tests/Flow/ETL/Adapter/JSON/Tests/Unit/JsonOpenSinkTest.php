<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit;

use Flow\ETL\Adapter\JSON\JsonFraming;
use Flow\ETL\Adapter\JSON\JsonOpenSink;
use Flow\ETL\Adapter\JSON\Tests\Mother\JsonEncoderMother;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Stream\StringDestinationStream;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;

final class JsonOpenSinkTest extends FlowTestCase
{
    #[TestWith([JsonFraming::Lines, "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n{\"id\":4}\n"])]
    #[TestWith([JsonFraming::Array, '[{"id":1},{"id":2},{"id":3},{"id":4}]'])]
    #[TestWith([JsonFraming::ArrayLines, "[\n{\"id\":1},\n{\"id\":2},\n{\"id\":3},\n{\"id\":4}\n]"])]
    public function test_three_batches_under_a_framing(JsonFraming $framing, string $expected): void
    {
        $stream = new StringDestinationStream(path('memory://out.json'));
        $sink = new JsonOpenSink($stream, JsonEncoderMother::php(), $framing);

        $sink->write(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $sink->write(array_to_rows([['id' => 3]], schema(int_schema('id'))));
        $sink->write(array_to_rows([['id' => 4]], schema(int_schema('id'))));
        $sink->close();

        static::assertSame($expected, $stream->content());
    }

    #[TestWith([JsonFraming::Lines])]
    #[TestWith([JsonFraming::Array])]
    #[TestWith([JsonFraming::ArrayLines])]
    public function test_close_without_a_write_appends_nothing(JsonFraming $framing): void
    {
        $stream = new StringDestinationStream(path('memory://out.json'));

        (new JsonOpenSink($stream, JsonEncoderMother::php(), $framing))->close();

        static::assertSame('', $stream->content());
    }

    #[TestWith([JsonFraming::Lines, ''])]
    #[TestWith([JsonFraming::Array, '[]'])]
    #[TestWith([JsonFraming::ArrayLines, "[\n\n]"])]
    public function test_only_empty_batches_close_an_empty_document(JsonFraming $framing, string $expected): void
    {
        $stream = new StringDestinationStream(path('memory://out.json'));
        $sink = new JsonOpenSink($stream, JsonEncoderMother::php(), $framing);

        $sink->write(array_to_rows([], schema(int_schema('id'))));
        $sink->write(array_to_rows([], schema(int_schema('id'))));
        $sink->close();

        static::assertSame($expected, $stream->content());
    }

    #[TestWith([JsonFraming::Lines, "{\"id\":1}\n{\"id\":2}\n"])]
    #[TestWith([JsonFraming::Array, '[{"id":1},{"id":2}]'])]
    public function test_an_empty_batch_between_batches_leaves_no_trace(JsonFraming $framing, string $expected): void
    {
        $stream = new StringDestinationStream(path('memory://out.json'));
        $sink = new JsonOpenSink($stream, JsonEncoderMother::php(), $framing);

        $sink->write(array_to_rows([], schema(int_schema('id'))));
        $sink->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $sink->write(array_to_rows([], schema(int_schema('id'))));
        $sink->write(array_to_rows([['id' => 2]], schema(int_schema('id'))));
        $sink->close();

        static::assertSame($expected, $stream->content());
    }
}
