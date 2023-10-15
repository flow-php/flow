<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech\Tests\Integration;

use Flow\ETL\Config;
use Flow\ETL\DSL\Avro;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class AvroTest extends TestCase
{
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
                            Entry::string('string', 'name_' . $i),
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
            ->parallelize(10)
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
