<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\Double\FakeRandomOrdersExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\SizeUnits;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Ramsey\Uuid\Uuid;

use function array_diff;
use function extension_loaded;
use function file_exists;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;
use function is_dir;
use function is_file;
use function rmdir;
use function scandir;
use function unlink;

final class ParquetTest extends FlowTestCase
{
    public function test_writing_and_reading_into_parquet(): void
    {
        $path = path('memory://var/file.snappy.parquet');

        $config = config();
        data_frame($config)
            ->read(new FakeExtractor(10))
            ->drop('null', 'array', 'object', 'enum')
            ->write(to_parquet($path))
            ->run();

        static::assertEquals(10, data_frame($config)->read(from_parquet($path))->count());
    }

    public function test_writing_and_reading_with_explicit_arrow_engine(): void
    {
        if (!extension_loaded('arrow')) {
            static::markTestSkipped('arrow extension is not loaded');
        }

        $path = path('memory://var/arrow_engine.parquet');
        $config = config();

        data_frame($config)
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(to_parquet($path, engine: new ArrowParquetEngine()))
            ->run();

        static::assertSame(
            2,
            data_frame($config)->read(from_parquet($path, engine: new ArrowParquetEngine()))->count(),
        );
    }

    public function test_writing_and_reading_with_explicit_php_engine(): void
    {
        $path = path('memory://var/php_engine.parquet');
        $config = config();

        data_frame($config)
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(to_parquet($path, engine: new PhpParquetEngine()))
            ->run();

        static::assertSame(2, data_frame($config)->read(from_parquet($path, engine: new PhpParquetEngine()))->count());
    }

    public function test_writing_and_reading_parquet_orders(): void
    {
        $path = path(__DIR__ . '/var/orders.snappy.parquet');
        $config = config();
        data_frame($config)
            ->read(new FakeRandomOrdersExtractor(1000))
            ->mode(overwrite())
            ->write(to_parquet($path)->withOptions(Options::default()->set(
                Option::ROW_GROUP_SIZE_CHECK_INTERVAL,
                500,
            )->set(Option::ROW_GROUP_SIZE_BYTES, SizeUnits::MiB_SIZE)->set(Option::PAGE_MAXIMUM_ROWS_COUNT, 10)))
            ->run();

        static::assertEquals(1000, data_frame($config)->read(from_parquet($path))->count());
    }

    public function test_writing_with_provided_schema(): void
    {
        $path = path('memory://var/file_schema.snappy.parquet');
        $config = config();
        data_frame($config)
            ->read(from_array([
                [
                    'id' => 1,
                    'name' => 'test',
                    'uuid' => Uuid::fromString('26fd21b0-6080-4d6c-bdb4-1214f1feffef'),
                    'json' => '[{"id":1,"name":"test"},{"id":2,"name":"test"}]',
                ],
                [
                    'id' => 2,
                    'name' => 'test',
                    'uuid' => Uuid::fromString('26fd21b0-6080-4d6c-bdb4-1214f1feffef'),
                    'json' => '[{"id":1,"name":"test"},{"id":2,"name":"test"}]',
                ],
            ]))
            ->write(to_parquet($path, schema: schema(
                str_schema('id'),
                str_schema('name'),
                str_schema('uuid'),
                json_schema('json'),
            )))
            ->run();

        static::assertEquals(
            [
                [
                    'id' => '1',
                    'name' => 'test',
                    'uuid' => '26fd21b0-6080-4d6c-bdb4-1214f1feffef',
                    'json' => [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']],
                ],
                [
                    'id' => '2',
                    'name' => 'test',
                    'uuid' => '26fd21b0-6080-4d6c-bdb4-1214f1feffef',
                    'json' => [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']],
                ],
            ],
            data_frame($config)->read(from_parquet($path))->fetch()->toArray(),
        );

        static::assertTrue($config->fstab()->for($path)->status($path)?->isFile());
    }

    /**
     * @param string $path
     */
    private function cleanDirectory(string $path): void
    {
        if (file_exists($path) && is_dir($path)) {
            $scanResult = scandir($path);

            if ($scanResult === false) {
                return;
            }

            $files = array_diff($scanResult, ['..', '.']);

            foreach ($files as $file) {
                if (is_file($path . DIRECTORY_SEPARATOR . $file)) {
                    $this->removeFile($path . DIRECTORY_SEPARATOR . $file);
                } else {
                    $this->cleanDirectory($path . DIRECTORY_SEPARATOR . $file);
                }
            }

            rmdir($path);
        }
    }

    /**
     * @param string $path
     */
    private function removeFile(string $path): void
    {
        if (file_exists($path)) {
            if (is_dir($path)) {
                $this->cleanDirectory($path);
            } else {
                unlink($path);
            }
        }
    }
}
