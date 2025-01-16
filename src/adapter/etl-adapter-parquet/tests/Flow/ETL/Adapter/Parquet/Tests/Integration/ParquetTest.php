<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use function Flow\ETL\Adapter\Parquet\{from_parquet, to_parquet};
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{df, from_array, json_schema, schema, str_schema};
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\{Tests\FlowTestCase};
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\Reader;
use Ramsey\Uuid\Uuid;

final class ParquetTest extends FlowTestCase
{
    public function test_writing_to_file() : void
    {
        $path = __DIR__ . '/var/file.snappy.parquet';
        $this->removeFile($path);

        df()
            ->read(new FakeExtractor(10))
            ->drop('null', 'array', 'object', 'enum')
            ->write(to_parquet($path))
            ->run();

        self::assertEquals(
            10,
            (data_frame())
                ->read(from_parquet($path))
                ->count()
        );

        $parquetFile = (new Reader())->read($path);
        self::assertNotEmpty($parquetFile->metadata()->columnChunks());

        foreach ($parquetFile->metadata()->columnChunks() as $columnChunk) {
            self::assertSame(Compressions::SNAPPY, $columnChunk->codec());
        }

        self::assertFileExists($path);
        $this->removeFile($path);
    }

    public function test_writing_with_provided_schema() : void
    {
        $path = __DIR__ . '/var/file_schema.snappy.parquet';
        $this->removeFile($path);

        df()
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
            df()
                ->read(from_parquet($path))
                ->fetch()
                ->toArray()
        );

        self::assertFileExists($path);
        $this->removeFile($path);
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
