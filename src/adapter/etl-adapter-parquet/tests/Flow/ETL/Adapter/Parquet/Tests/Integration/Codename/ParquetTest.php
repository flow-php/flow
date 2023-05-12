<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration\Codename;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\Parquet;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class ParquetTest extends TestCase
{
    public function test_using_pattern_path() : void
    {
        $this->expectExceptionMessage("ParquetLoader path can't be pattern, given: /path/*/pattern.parquet");

        Parquet::to(new Path('/path/*/pattern.parquet'));
    }

    public function test_writing_and_reading_only_given_fields() : void
    {
        $this->removeFile($path = \sys_get_temp_dir() . '/file.parquet');

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
            ->write(Parquet::to($path))
            ->run();

        $this->assertFileExists($path);

        $this->assertEquals(
            new Rows(
                ...\array_map(function (int $i) : Row {
                    return Row::create(Entry::integer('integer', $i));
                }, \range(1, 100))
            ),
            (new Flow())
                ->read(Parquet::from($path, 'row', ['integer']))
                ->withEntry('unpacked', ref('row')->unpack())
                ->renameAll('unpacked.', '')
                ->drop('row')
                ->fetch()
        );

        $this->removeFile($path);
    }

    public function test_writing_and_reading_parquet_with_all_supported_types() : void
    {
        $this->removeFile($path = __DIR__ . '/file.parquet');

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
            ->write(Parquet::to($path, 10))
            ->run();

        $this->assertFileExists($path);

        $this->assertEquals(
            $rows,
            (new Flow())
                ->read(Parquet::from($path))
                ->withEntry('unpacked', ref('row')->unpack())
                ->renameAll('unpacked.', '')
                ->drop('row')
                ->fetch()
        );

        $this->removeFile($path);
    }

    public function test_writing_safe_and_reading_parquet_with_all_supported_types() : void
    {
        $this->cleanDirectory($path = \sys_get_temp_dir() . '/directory.parquet');

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
            ->write(Parquet::to($path, 50))
            ->run();

        $this->assertFileExists($path);

        $paths = \array_map(
            fn (string $fileName) : Path => new Path($path . '/' . $fileName),
            \array_values(\array_diff(\scandir($path), ['..', '.']))
        );

        $this->assertEquals(
            $rows,
            (new Flow())
                ->read(Parquet::from($paths))
                ->withEntry('unpacked', ref('row')->unpack())
                ->renameAll('unpacked.', '')
                ->drop('row')
                ->sortBy(ref('integer'))
                ->fetch()
        );

        $this->cleanDirectory($path);
    }

    /**
     * @param string $path
     */
    private function cleanDirectory(string $path) : void
    {
        if (\file_exists($path) && \is_dir($path)) {
            $files = \array_values(\array_diff(\scandir($path), ['..', '.']));

            foreach ($files as $file) {
                if (\is_file($path . DIRECTORY_SEPARATOR . $file)) {
                    $this->removeFile($path . DIRECTORY_SEPARATOR . $file);
                } else {
                    $this->cleanDirectory($path . DIRECTORY_SEPARATOR . $file);
                }
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
