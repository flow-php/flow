<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\Tests\Integration;

use function Flow\ETL\DSL\Adapter\Avro\from_avro;
use function Flow\ETL\DSL\Adapter\Avro\to_avro;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\ignore;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\json_object_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\struct_element;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\struct_type;
use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_object;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\Adapter\Avro\FlixTech\AvroExtractor;
use Flow\ETL\Config;
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

        (new Flow())->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->write(to_avro($path))
            ->run();

        $extractor = new AvroExtractor(Path::realpath($path));
        $extractor->changeLimit(2);

        $this->assertCount(
            2,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_safe_writing_and_reading_avro_with_all_supported_types() : void
    {
        $this->cleanDirectory($path = \sys_get_temp_dir() . '/directory.avro');

        df()
            ->read(from_rows(
                $rows = new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            int_entry('integer', $i),
                            float_entry('float', 1.5),
                            str_entry('string', 'name_' . $i),
                            bool_entry('boolean', true),
                            datetime_entry('datetime', new \DateTimeImmutable()),
                            json_object_entry('json_object', ['id' => 1, 'name' => 'test']),
                            json_entry('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            list_entry('list_of_strings', ['a', 'b', 'c'], type_list(type_string())),
                            list_entry('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()], type_list(type_object(\DateTimeImmutable::class)))
                        );
                    }, \range(1, 100))
                )
            ))
            ->appendSafe()
            ->write(to_avro($path))
            ->run();

        $paths = \array_map(
            fn (string $fileName) : Path => new Path($path . '/' . $fileName),
            \array_values(\array_diff(\scandir($path), ['..', '.']))
        );

        $this->assertFileExists($path);

        $this->assertEquals(
            $rows,
            (new Flow())
                ->read(from_avro($paths))
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

        (new Flow())->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->write(to_avro($path))
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

        to_avro(new Path('/path/*/pattern.avro'));
    }

    public function test_writing_and_reading_avro_with_all_supported_types() : void
    {
        $this->removeFile($path = \sys_get_temp_dir() . '/file.avro');

        df()
            ->read(from_rows(
                $rows = new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            int_entry('integer', $i),
                            float_entry('float', 1.5),
                            $i % 10 === 0 ? null_entry('string') : str_entry('string', 'name_' . $i),
                            bool_entry('boolean', true),
                            datetime_entry('datetime', new \DateTimeImmutable()),
                            json_object_entry('json_object', ['id' => 1, 'name' => 'test']),
                            json_entry('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            list_entry('list_of_strings', ['a', 'b', 'c'], type_list(type_string())),
                            list_entry('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()], type_list(type_object(\DateTimeImmutable::class))),
                            struct_entry(
                                'address',
                                [
                                    'street' => 'street_' . $i,
                                    'city' => 'city_' . $i,
                                    'zip' => 'zip_' . $i,
                                    'country' => 'country_' . $i,
                                    'location' => ['lat' => 1.5, 'lon' => 1.5],
                                ],
                                struct_type(
                                    struct_element('street', type_string()),
                                    struct_element('city', type_string()),
                                    struct_element('zip', type_string()),
                                    struct_element('country', type_string()),
                                    struct_element(
                                        'location',
                                        struct_type(
                                            struct_element('lat', type_float()),
                                            struct_element('lon', type_float()),
                                        )
                                    )
                                ),
                            ),
                        );
                    }, \range(1, 100))
                )
            ))
            ->batchSize(10)
            ->write(to_avro($path))
            ->run();

        $this->assertFileExists($path);

        $this->assertEquals(
            $rows,
            Flow::setUp(Config::builder()->putInputIntoRows()->build())
                ->read(from_avro($path))
                ->drop('_input_file_uri')
                ->fetch()
        );

        $this->removeFile($path);
    }

    public function test_writing_twice_to_the_same_location() : void
    {
        $this->removeFile($path = \sys_get_temp_dir() . '/file.avro');

        $this->expectExceptionMessage('please change path to different or set different SaveMode');

        df()
            ->read(from_rows(
                $rows = new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            int_entry('integer', $i),
                            float_entry('float', 1.5),
                            str_entry('string', 'name_' . $i),
                            bool_entry('boolean', true),
                            datetime_entry('datetime', new \DateTimeImmutable()),
                            json_object_entry('json_object', ['id' => 1, 'name' => 'test']),
                            json_entry('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            list_entry('list_of_strings', ['a', 'b', 'c'], type_list(type_string())),
                            list_entry('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()], type_list(type_object(\DateTimeImmutable::class)))
                        );
                    }, \range(1, 100))
                )
            ))
            ->write(to_avro($path))
            ->run();

        df()
            ->read(from_rows(
                $rows = new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            int_entry('integer', $i),
                            float_entry('float', 1.5),
                            str_entry('string', 'name_' . $i),
                            bool_entry('boolean', true),
                            datetime_entry('datetime', new \DateTimeImmutable()),
                            json_object_entry('json_object', ['id' => 1, 'name' => 'test']),
                            json_entry('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            list_entry('list_of_strings', ['a', 'b', 'c'], type_list(type_string())),
                            list_entry('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()], type_list(type_object(\DateTimeImmutable::class)))
                        );
                    }, \range(1, 100))
                )
            ))
            ->write(to_avro($path))
            ->run();

        $this->assertFileExists($path);

        $this->assertEquals(
            $rows,
            (new Flow())
                ->read(from_avro($path))
                ->fetch()
        );

        $this->removeFile($path);
    }

    public function test_writing_twice_to_the_same_location_with_ignore_mode() : void
    {
        $this->removeFile($path = \sys_get_temp_dir() . '/file.avro');

        df()
            ->read(from_rows(
                $rows = new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            int_entry('integer', $i),
                            float_entry('float', 1.5),
                            str_entry('string', 'name_' . $i),
                            bool_entry('boolean', true),
                            datetime_entry('datetime', new \DateTimeImmutable()),
                            json_object_entry('json_object', ['id' => 1, 'name' => 'test']),
                            json_entry('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            list_entry('list_of_strings', ['a', 'b', 'c'], type_list(type_string())),
                            list_entry('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()], type_list(type_object(\DateTimeImmutable::class)))
                        );
                    }, \range(1, 100))
                )
            ))
            ->write(to_avro($path))
            ->run();

        df()
            ->read(from_rows(
                new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            int_entry('integer', $i),
                            float_entry('float', 1.5),
                            str_entry('string', 'name_' . $i),
                            bool_entry('boolean', true),
                            datetime_entry('datetime', new \DateTimeImmutable()),
                            json_object_entry('json_object', ['id' => 1, 'name' => 'test']),
                            json_entry('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            list_entry('list_of_strings', ['a', 'b', 'c'], type_list(type_string())),
                            list_entry('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()], type_list(type_object(\DateTimeImmutable::class)))
                        );
                    }, \range(1, 100))
                )
            ))
            ->saveMode(ignore())
            ->write(to_avro($path))
            ->run();

        $this->assertFileExists($path);

        $this->assertEquals(
            $rows,
            (new Flow())
                ->read(from_avro($path))
                ->fetch()
        );

        $this->removeFile($path);
    }

    public function test_writing_twice_to_the_same_location_with_overwrite_mode() : void
    {
        $this->removeFile($path = \sys_get_temp_dir() . '/file.avro');

        df()
            ->read(from_rows(
                new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            int_entry('integer', $i),
                            float_entry('float', 1.5),
                            str_entry('string', 'name_' . $i),
                            bool_entry('boolean', true),
                            datetime_entry('datetime', new \DateTimeImmutable()),
                            json_object_entry('json_object', ['id' => 1, 'name' => 'test']),
                            json_entry('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            list_entry('list_of_strings', ['a', 'b', 'c'], type_list(type_string())),
                            list_entry('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()], type_list(type_object(\DateTimeImmutable::class)))
                        );
                    }, \range(1, 100))
                )
            ))
            ->write(to_avro($path))
            ->run();

        df()
            ->read(from_rows(
                $rows = new Rows(
                    ...\array_map(function (int $i) : Row {
                        return Row::create(
                            int_entry('integer', $i),
                            float_entry('float', 1.5),
                            str_entry('string', 'name_' . $i),
                            bool_entry('boolean', true),
                            datetime_entry('datetime', new \DateTimeImmutable()),
                            json_object_entry('json_object', ['id' => 1, 'name' => 'test']),
                            json_entry('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                            list_entry('list_of_strings', ['a', 'b', 'c'], type_list(type_string())),
                            list_entry('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()], type_list(type_object(\DateTimeImmutable::class)))
                        );
                    }, \range(1, 100))
                )
            ))
            ->mode(SaveMode::Overwrite)
            ->write(to_avro($path))
            ->run();

        $this->assertFileExists($path);

        $this->assertEquals(
            $rows,
            (new Flow())
                ->read(from_avro($path))
                ->fetch()
        );

        $this->removeFile($path);
    }

    /**
     * @param string $path
     */
    private function cleanDirectory(string $path) : void
    {
        if (\file_exists($path)) {
            if (\is_dir($path)) {
                $files = \array_values(\array_diff(\scandir($path), ['..', '.']));

                foreach ($files as $file) {
                    $this->removeFile($path . DIRECTORY_SEPARATOR . $file);
                }

                \rmdir($path);
            } else {
                \unlink($path);
            }
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
