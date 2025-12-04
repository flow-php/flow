<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\Tests\Integration;

use function Flow\ETL\DSL\Adapter\Avro\{from_avro, to_avro};
use function Flow\ETL\DSL\{config, flow_context};
use function Flow\Filesystem\DSL\{path, path_real};
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Tests\FlowTestCase;

final class AvroTest extends FlowTestCase
{
    #[\Override]
    protected function setUp() : void
    {
        self::markTestSkipped('Avro integration was abandoned due to lack of availability of good Avro libraries.');
    }

    public function test_limit() : void
    {
        $extractor = from_avro(path_real(__DIR__ . '/../Fixtures/orders_flow.avro'));
        $extractor->changeLimit(2);

        self::assertCount(
            2,
            \iterator_to_array($extractor->extract(flow_context(config())))
        );
    }

    public function test_signal_stop() : void
    {
        $extractor = from_avro(path_real(__DIR__ . '/../Fixtures/orders_flow.avro'));

        $generator = $extractor->extract(flow_context(config()));

        self::assertTrue($generator->valid());
        $generator->next();
        self::assertTrue($generator->valid());
        $generator->next();
        self::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        self::assertFalse($generator->valid());
    }

    public function test_using_pattern_path() : void
    {
        $this->expectExceptionMessage("AvroLoader path can't be pattern, given: /path/*/pattern.avro");

        to_avro(path('/path/*/pattern.avro'));
    }
}
