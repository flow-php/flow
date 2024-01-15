<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\json_object_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\struct_element;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\struct_type;
use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_object;
use function Flow\ETL\DSL\type_string;
use function Flow\ETL\DSL\uuid_entry;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;
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
            ->read(from_rows($rows = $this->createRows(10)))
            ->write(to_parquet($path))
            ->run();

        $this->assertEquals(
            $rows,
            (new Flow())
                ->read(from_parquet($path))
                ->fetch()
        );

        $parquetFile = (new Reader())->read($path);
        $this->assertNotEmpty($parquetFile->metadata()->columnChunks());

        foreach ($parquetFile->metadata()->columnChunks() as $columnChunk) {
            $this->assertSame(Compressions::SNAPPY, $columnChunk->codec());
        }

        $this->assertFileExists($path);
        $this->removeFile($path);
    }

    public function test_writing_with_partitioning() : void
    {
        $path = \sys_get_temp_dir() . '/partitioned';
        $this->cleanDirectory($path);

        df()
            ->read(from_rows($rows = new Rows(
                $this->createRow(1, new \DateTimeImmutable('2020-01-01 00:01:00')),
                $this->createRow(1, new \DateTimeImmutable('2020-01-01 00:02:00')),
                $this->createRow(1, new \DateTimeImmutable('2020-01-02 00:01:00')),
                $this->createRow(1, new \DateTimeImmutable('2020-01-02 00:02:00')),
                $this->createRow(1, new \DateTimeImmutable('2020-01-03 00:01:00')),
            )))
            ->withEntry('date', ref('datetime')->toDate()->dateFormat())
            ->partitionBy(ref('date'))
            ->write(to_parquet($path))
            ->run();

        $this->assertEquals(
            $rows,
            (new Flow())
                ->read(from_parquet($path . '/**/*.parquet'))
                ->drop('date')
                ->sortBy(ref('datetime')->asc())
                ->fetch()
        );

        $this->assertSame(
            ['date=2020-01-01', 'date=2020-01-02', 'date=2020-01-03'],
            $this->listDirectoryFiles($path)
        );
        $this->assertDirectoryExists($path);
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

    private function createRow(int $index, ?\DateTimeImmutable $dateTime = null) : Row
    {
        return Row::create(
            uuid_entry('uuid', Uuid::uuid4()->toString()),
            int_entry('integer', $index),
            float_entry('float', 1.5),
            str_entry('string', 'name_' . $index),
            bool_entry('boolean', true),
            datetime_entry('datetime', $dateTime ?: new \DateTimeImmutable()),
            json_object_entry('json_object', ['id' => 1, 'name' => 'test']),
            json_entry('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
            list_entry('list_of_strings', ['a', 'b', 'c'], type_list(type_string())),
            list_entry('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()], type_list(type_object(\DateTimeImmutable::class))),
            struct_entry(
                'address',
                [
                    'street' => 'street_' . $index,
                    'city' => 'city_' . $index,
                    'zip' => 'zip_' . $index,
                    'country' => 'country_' . $index,
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
    }

    private function createRows(int $count) : Rows
    {
        $rows = [];

        for ($i = 0; $i < $count; $i++) {
            $rows[] = $this->createRow($i);
        }

        return new Rows(...$rows);
    }

    private function listDirectoryFiles(string $path) : array
    {
        return \array_values(\array_diff(\scandir($path), ['.', '..']));
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
