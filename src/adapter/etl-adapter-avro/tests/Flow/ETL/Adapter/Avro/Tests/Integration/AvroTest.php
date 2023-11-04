<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\Tests\Integration;

use Flow\ETL\Adapter\Avro\FlixTech\AvroExtractor;
use Flow\ETL\Config;
use Flow\ETL\DSL\Avro;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class AvroTest extends TestCase
{
    public function test_limit() : void
    {
        $path = \sys_get_temp_dir() . '/avro_extractor_signal_stop.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow())->read(From::array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->write(Avro::to($path))
            ->run();

        $extractor = new AvroExtractor(Path::realpath($path));
        $extractor->changeLimit(2);

        $this->assertCount(
            2,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_partitioning() : void
    {
        $this->expectExceptionMessage('Partitioning is not supported yet');

        $this->removeFile($path = \sys_get_temp_dir() . '/file.avro');

        (new Flow)
            ->read(From::rows(
                $rows = new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            Entry::integer('integer', $i),
                            Entry::float('float', 1.5),
                            Entry::string('string', 'name_' . $i),
                            Entry::boolean('boolean', true),
                            Entry::datetime('datetime', new \DateTimeImmutable()),
                            Entry::json_object('json_object', ['id' => 1, 'name' => 'test']),
                            Entry::json('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            Entry::list_of_string('list_of_strings', ['a', 'b', 'c']),
                            Entry::list_of_datetime('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()])
                        );
                    }, \range(1, 100))
                )
            ))
            ->partitionBy('integer')
            ->write(Avro::to($path))
            ->run();
    }

    public function test_safe_writing_and_reading_avro_with_all_supported_types() : void
    {
        $this->cleanDirectory($path = \sys_get_temp_dir() . '/directory.avro');

        (new Flow)
            ->read(From::rows(
                $rows = new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            Entry::integer('integer', $i),
                            Entry::float('float', 1.5),
                            Entry::string('string', 'name_' . $i),
                            Entry::boolean('boolean', true),
                            Entry::datetime('datetime', new \DateTimeImmutable()),
                            Entry::json_object('json_object', ['id' => 1, 'name' => 'test']),
                            Entry::json('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            Entry::list_of_string('list_of_strings', ['a', 'b', 'c']),
                            Entry::list_of_datetime('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()])
                        );
                    }, \range(1, 100))
                )
            ))
            ->threadSafe()
            ->write(Avro::to($path))
            ->run();

        $paths = \array_map(
            fn (string $fileName) : Path => new Path($path . '/' . $fileName),
            \array_values(\array_diff(\scandir($path), ['..', '.']))
        );

        $this->assertFileExists($path);

        $this->assertEquals(
            $rows,
            (new Flow())
                ->read(Avro::from($paths))
                ->fetch()
        );

        $this->cleanDirectory($path);
    }

    public function test_signal_stop() : void
    {
        $path = \sys_get_temp_dir() . '/avro_extractor_signal_stop.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow())->read(From::array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->write(Avro::to($path))
            ->run();

        $extractor = new AvroExtractor(Path::realpath($path));

        $generator = $extractor->extract(new FlowContext(Config::default()));

        $this->assertSame([['id' => 1]], $generator->current()->toArray());
        $this->assertTrue($generator->valid());
        $generator->next();
        $this->assertSame([['id' => 2]], $generator->current()->toArray());
        $this->assertTrue($generator->valid());
        $generator->next();
        $this->assertSame([['id' => 3]], $generator->current()->toArray());
        $this->assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        $this->assertFalse($generator->valid());
    }

    public function test_using_pattern_path() : void
    {
        $this->expectExceptionMessage("AvroLoader path can't be pattern, given: /path/*/pattern.avro");

        Avro::to(new Path('/path/*/pattern.avro'));
    }

    public function test_writing_and_reading_avro_with_all_supported_types() : void
    {
        $this->removeFile($path = \sys_get_temp_dir() . '/file.avro');

        (new Flow)
            ->read(From::rows(
                $rows = new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            Entry::integer('integer', $i),
                            Entry::float('float', 1.5),
                            $i % 10 === 0 ? Entry::null('string') : Entry::string('string', 'name_' . $i),
                            Entry::boolean('boolean', true),
                            Entry::datetime('datetime', new \DateTimeImmutable()),
                            Entry::json_object('json_object', ['id' => 1, 'name' => 'test']),
                            Entry::json('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            Entry::list_of_string('list_of_strings', ['a', 'b', 'c']),
                            Entry::list_of_datetime('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()]),
                            Entry::structure(
                                'address',
                                Entry::string('street', 'street_' . $i),
                                Entry::string('city', 'city_' . $i),
                                Entry::string('zip', 'zip_' . $i),
                                Entry::string('country', 'country_' . $i),
                                Entry::structure(
                                    'location',
                                    Entry::float('lat', 1.5),
                                    Entry::float('lon', 1.5)
                                )
                            ),
                        );
                    }, \range(1, 100))
                )
            ))
            ->batchSize(10)
            ->write(Avro::to($path))
            ->run();

        $this->assertFileExists($path);

        $this->assertEquals(
            $rows,
            Flow::setUp(Config::builder()->putInputIntoRows()->build())
                ->read(Avro::from($path))
                ->drop('_input_file_uri')
                ->fetch()
        );

        $this->removeFile($path);
    }

    public function test_writing_twice_to_the_same_location() : void
    {
        $this->removeFile($path = \sys_get_temp_dir() . '/file.avro');

        $this->expectExceptionMessage('please change path to different or set different SaveMode');

        (new Flow)
            ->read(From::rows(
                $rows = new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            Entry::integer('integer', $i),
                            Entry::float('float', 1.5),
                            Entry::string('string', 'name_' . $i),
                            Entry::boolean('boolean', true),
                            Entry::datetime('datetime', new \DateTimeImmutable()),
                            Entry::json_object('json_object', ['id' => 1, 'name' => 'test']),
                            Entry::json('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            Entry::list_of_string('list_of_strings', ['a', 'b', 'c']),
                            Entry::list_of_datetime('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()])
                        );
                    }, \range(1, 100))
                )
            ))
            ->write(Avro::to($path))
            ->run();

        (new Flow)
            ->read(From::rows(
                $rows = new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            Entry::integer('integer', $i),
                            Entry::float('float', 1.5),
                            Entry::string('string', 'name_' . $i),
                            Entry::boolean('boolean', true),
                            Entry::datetime('datetime', new \DateTimeImmutable()),
                            Entry::json_object('json_object', ['id' => 1, 'name' => 'test']),
                            Entry::json('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            Entry::list_of_string('list_of_strings', ['a', 'b', 'c']),
                            Entry::list_of_datetime('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()])
                        );
                    }, \range(1, 100))
                )
            ))
            ->write(Avro::to($path))
            ->run();

        $this->assertFileExists($path);

        $this->assertEquals(
            $rows,
            (new Flow())
                ->read(Avro::from($path))
                ->fetch()
        );

        $this->removeFile($path);
    }

    public function test_writing_twice_to_the_same_location_with_ignore_mode() : void
    {
        $this->removeFile($path = \sys_get_temp_dir() . '/file.avro');

        (new Flow)
            ->read(From::rows(
                $rows = new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            Entry::integer('integer', $i),
                            Entry::float('float', 1.5),
                            Entry::string('string', 'name_' . $i),
                            Entry::boolean('boolean', true),
                            Entry::datetime('datetime', new \DateTimeImmutable()),
                            Entry::json_object('json_object', ['id' => 1, 'name' => 'test']),
                            Entry::json('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            Entry::list_of_string('list_of_strings', ['a', 'b', 'c']),
                            Entry::list_of_datetime('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()])
                        );
                    }, \range(1, 100))
                )
            ))
            ->write(Avro::to($path))
            ->run();

        (new Flow)
            ->read(From::rows(
                new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            Entry::integer('integer', $i),
                            Entry::float('float', 1.5),
                            Entry::string('string', 'name_' . $i),
                            Entry::boolean('boolean', true),
                            Entry::datetime('datetime', new \DateTimeImmutable()),
                            Entry::json_object('json_object', ['id' => 1, 'name' => 'test']),
                            Entry::json('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            Entry::list_of_string('list_of_strings', ['a', 'b', 'c']),
                            Entry::list_of_datetime('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()])
                        );
                    }, \range(1, 100))
                )
            ))
            ->mode(SaveMode::Ignore)
            ->write(Avro::to($path))
            ->run();

        $this->assertFileExists($path);

        $this->assertEquals(
            $rows,
            (new Flow())
                ->read(Avro::from($path))
                ->fetch()
        );

        $this->removeFile($path);
    }

    public function test_writing_twice_to_the_same_location_with_overwrite_mode() : void
    {
        $this->removeFile($path = \sys_get_temp_dir() . '/file.avro');

        (new Flow)
            ->read(From::rows(
                new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            Entry::integer('integer', $i),
                            Entry::float('float', 1.5),
                            Entry::string('string', 'name_' . $i),
                            Entry::boolean('boolean', true),
                            Entry::datetime('datetime', new \DateTimeImmutable()),
                            Entry::json_object('json_object', ['id' => 1, 'name' => 'test']),
                            Entry::json('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            Entry::list_of_string('list_of_strings', ['a', 'b', 'c']),
                            Entry::list_of_datetime('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()])
                        );
                    }, \range(1, 100))
                )
            ))
            ->write(Avro::to($path))
            ->run();

        (new Flow)
            ->read(From::rows(
                $rows = new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            Entry::integer('integer', $i),
                            Entry::float('float', 1.5),
                            Entry::string('string', 'name_' . $i),
                            Entry::boolean('boolean', true),
                            Entry::datetime('datetime', new \DateTimeImmutable()),
                            Entry::json_object('json_object', ['id' => 1, 'name' => 'test']),
                            Entry::json('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            Entry::list_of_string('list_of_strings', ['a', 'b', 'c']),
                            Entry::list_of_datetime('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()])
                        );
                    }, \range(1, 100))
                )
            ))
            ->mode(SaveMode::Overwrite)
            ->write(Avro::to($path))
            ->run();

        $this->assertFileExists($path);

        $this->assertEquals(
            $rows,
            (new Flow())
                ->read(Avro::from($path))
                ->fetch()
        );

        $this->removeFile($path);
    }

    /**
     * @param string $path
     */
    private function cleanDirectory(string $path) : void
    {
        if (\file_exists($path) && \is_dir($path)) {
            $files = \array_values(\array_diff(\scandir($path), ['..', '.']));

            foreach ($files as $file) {
                $this->removeFile($path . DIRECTORY_SEPARATOR . $file);
            }

            \rmdir($path);
        }
    }

    /**
     * @param string $path
     */
    private function removeFile(string $path) : void
    {
        if (\file_exists($path)) {
            \unlink($path);
        }
    }
}
