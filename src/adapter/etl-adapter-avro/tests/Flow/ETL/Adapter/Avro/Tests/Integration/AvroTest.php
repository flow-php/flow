<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\Tests\Integration;

use function Flow\ETL\DSL\Adapter\Avro\{from_avro, to_avro};
use function Flow\ETL\DSL\{df, from_array, lit, type_map, type_string};
use Flow\ETL\Adapter\Avro\FlixTech\AvroExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\{Config, Flow, FlowContext};
use PHPUnit\Framework\TestCase;

final class AvroTest extends TestCase
{
    public function test_limit() : void
    {
        $path = \sys_get_temp_dir() . '/avro_extractor_signal_stop.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->write(to_avro($path))
            ->run();

        $extractor = new AvroExtractor(Path::realpath($path));
        $extractor->changeLimit(2);

        self::assertCount(
            2,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_signal_stop() : void
    {
        $path = \sys_get_temp_dir() . '/avro_extractor_signal_stop.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->write(to_avro($path))
            ->run();

        $extractor = new AvroExtractor(Path::realpath($path));

        $generator = $extractor->extract(new FlowContext(Config::default()));

        self::assertSame([['id' => 1]], $generator->current()->toArray());
        self::assertTrue($generator->valid());
        $generator->next();
        self::assertSame([['id' => 2]], $generator->current()->toArray());
        self::assertTrue($generator->valid());
        $generator->next();
        self::assertSame([['id' => 3]], $generator->current()->toArray());
        self::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        self::assertFalse($generator->valid());
    }

    public function test_using_pattern_path() : void
    {
        $this->expectExceptionMessage("AvroLoader path can't be pattern, given: /path/*/pattern.avro");

        to_avro(new Path('/path/*/pattern.avro'));
    }

    public function test_writing_and_reading_avro_with_all_supported_types() : void
    {
        $this->removeFile($path = \sys_get_temp_dir() . '/file.avro');

        df()
            ->read(new FakeExtractor(100))
            ->drop('null', 'array', 'object', 'enum', 'map')
            // avro maps support only string keys
            ->withEntry('map', lit(['0' => 'zero', '1' => 'one'])->cast(type_map(type_string(), type_string())))
            ->batchSize(10)
            ->write(to_avro($path))
            ->run();

        self::assertFileExists($path);

        self::assertEquals(
            100,
            Flow::setUp(Config::builder()->putInputIntoRows()->build())
                ->read(from_avro($path))
                ->drop('_input_file_uri')
                ->count()
        );

        $this->removeFile($path);
    }

    /**
     * @param string $path
     */
    private function removeFile(string $path) : void
    {
        if (\file_exists($path) && !\is_dir($path)) {
            \unlink($path);
        }
    }
}
