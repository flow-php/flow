<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\Tests\Integration;

use function Flow\ETL\DSL\Adapter\Avro\{to_avro};
use Flow\ETL\Adapter\Avro\FlixTech\AvroExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\{Config, FlowContext};
use Flow\Filesystem\Path;
use PHPUnit\Framework\TestCase;

final class AvroTest extends TestCase
{
    protected function setUp() : void
    {
        self::markTestSkipped('Avro integration was abandoned due to lack of availability of good Avro libraries.');
    }

    public function test_limit() : void
    {
        $extractor = new AvroExtractor(Path::realpath(__DIR__ . '/../Fixtures/orders_flow.avro'));
        $extractor->changeLimit(2);

        self::assertCount(
            2,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_signal_stop() : void
    {
        $extractor = new AvroExtractor(Path::realpath(__DIR__ . '/../Fixtures/orders_flow.avro'));

        $generator = $extractor->extract(new FlowContext(Config::default()));

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

        to_avro(new Path('/path/*/pattern.avro'));
    }
}
