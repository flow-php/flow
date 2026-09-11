<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\Tests\Integration;

use Flow\ETL\Extractor\Signal;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\Adapter\Avro\from_avro;
use function Flow\ETL\DSL\Adapter\Avro\to_avro;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;

final class AvroTest extends FlowTestCase
{
    protected function setUp(): void
    {
        self::markTestSkipped('Avro integration was abandoned due to lack of availability of good Avro libraries.');
    }

    public function test_signal_stop(): void
    {
        $extractor = from_avro(path_real(__DIR__ . '/../Fixtures/orders_flow.avro'));

        $generator = $extractor->extract(flow_context(config()));

        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }

    public function test_using_pattern_path(): void
    {
        $this->expectExceptionMessage("AvroLoader path can't be pattern, given: /path/*/pattern.avro");

        to_avro(path('/path/*/pattern.avro'));
    }
}
