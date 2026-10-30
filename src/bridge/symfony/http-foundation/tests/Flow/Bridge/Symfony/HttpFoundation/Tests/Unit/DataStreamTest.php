<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Unit;

use Flow\Bridge\Symfony\HttpFoundation\DataStream;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function Flow\Bridge\Symfony\HttpFoundation\http_stream_open;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;

final class DataStreamTest extends TestCase
{
    public function test_a_config_is_refused_for_a_data_frame_source(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A DataFrame source carries its own config');

        http_stream_open(df()->read(from_array([['id' => 1]])))->config(config_builder());
    }

    public function test_a_config_is_taken_for_an_extractor_source(): void
    {
        $stream = DataStream::open(from_array([['id' => 1]]));

        static::assertSame($stream, $stream->config(config_builder()));
    }
}
