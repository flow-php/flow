<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use function Flow\ETL\Adapter\Parquet\{from_parquet, to_parquet};
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{config, from_array, json_schema, schema, str_schema};
use function Flow\Filesystem\DSL\{path};
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\{Tests\FlowTestCase};
use Ramsey\Uuid\Uuid;

final class ParquetTest extends FlowTestCase
{
    public function test_writing_and_reading_into_parquet() : void
    {
        $path = path('memory://var/file.snappy.parquet');

        $config = config();
        data_frame($config)
            ->read(new FakeExtractor(10))
            ->drop('null', 'array', 'object', 'enum')
            ->write(to_parquet($path))
            ->run();

        self::assertEquals(
            10,
            (data_frame($config))
                ->read(from_parquet($path))
                ->count()
        );
    }

    public function test_writing_with_provided_schema() : void
    {
        $path = path('memory://var/file_schema.snappy.parquet');
        $config = config();
        data_frame($config)
            ->read(from_array([
                ['id' => 1, 'name' => 'test', 'uuid' => Uuid::fromString('26fd21b0-6080-4d6c-bdb4-1214f1feffef'), 'json' => '[{"id":1,"name":"test"},{"id":2,"name":"test"}]'],
                ['id' => 2, 'name' => 'test', 'uuid' => Uuid::fromString('26fd21b0-6080-4d6c-bdb4-1214f1feffef'), 'json' => '[{"id":1,"name":"test"},{"id":2,"name":"test"}]'],
            ]))
            ->write(
                to_parquet($path, schema: schema(
                    str_schema('id'),
                    str_schema('name'),
                    str_schema('uuid'),
                    json_schema('json'),
                ))
            )
            ->run();

        self::assertEquals(
            [
                ['id' => '1', 'name' => 'test', 'uuid' => new \Flow\ETL\PHP\Value\Uuid('26fd21b0-6080-4d6c-bdb4-1214f1feffef'), 'json' => [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]],
                ['id' => '2', 'name' => 'test', 'uuid' => new \Flow\ETL\PHP\Value\Uuid('26fd21b0-6080-4d6c-bdb4-1214f1feffef'), 'json' => [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]],
            ],
            data_frame($config)
                ->read(from_parquet($path))
                ->fetch()
                ->toArray()
        );

        self::assertTrue($config->fstab()->for($path)->status($path)?->isFile());
    }

    /**
     * @param string $path
     */
    private function cleanDirectory(string $path) : void
    {
        if (\file_exists($path) && \is_dir($path)) {

            $files = \array_diff(\scandir($path), ['..', '.']);

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
            if (\is_dir($path)) {
                $this->cleanDirectory($path);
            } else {
                \unlink($path);
            }
        }
    }
}
