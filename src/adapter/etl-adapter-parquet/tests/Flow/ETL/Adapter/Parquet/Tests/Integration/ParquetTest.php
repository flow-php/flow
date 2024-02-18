<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\Reader;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;

final class ParquetTest extends TestCase
{
    public function test_writing_to_file() : void
    {
        $path = \sys_get_temp_dir() . '/file.snappy.parquet';
        $this->removeFile($path);

        df()
            ->read(new FakeExtractor(10))
            ->drop('null', 'array', 'object', 'enum')
            ->write(to_parquet($path))
            ->run();

        $this->assertEquals(
            10,
            (new Flow())
                ->read(from_parquet($path))
                ->count()
        );

        $parquetFile = (new Reader())->read($path);
        $this->assertNotEmpty($parquetFile->metadata()->columnChunks());

        foreach ($parquetFile->metadata()->columnChunks() as $columnChunk) {
            $this->assertSame(Compressions::SNAPPY, $columnChunk->codec());
        }

        $this->assertFileExists($path);
        $this->removeFile($path);
    }

    public function test_writing_with_provided_schema() : void
    {
        $path = \sys_get_temp_dir() . '/file_schema.snappy.parquet';
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

        $this->assertEquals(
            [
                ['id' => '1', 'name' => 'test', 'uuid' => new Row\Entry\Type\Uuid('26fd21b0-6080-4d6c-bdb4-1214f1feffef'), 'json' => '[{"id":1,"name":"test"},{"id":2,"name":"test"}]'],
                ['id' => '2', 'name' => 'test', 'uuid' => new Row\Entry\Type\Uuid('26fd21b0-6080-4d6c-bdb4-1214f1feffef'), 'json' => '[{"id":1,"name":"test"},{"id":2,"name":"test"}]'],
            ],
            df()
                ->read(from_parquet($path))
                ->fetch()
                ->toArray()
        );

        $this->assertFileExists($path);
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
