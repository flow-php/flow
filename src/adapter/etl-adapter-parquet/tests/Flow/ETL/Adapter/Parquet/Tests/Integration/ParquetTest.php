<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\Parquet;
use Flow\ETL\Flow;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\Reader;
use PHPUnit\Framework\TestCase;

final class ParquetTest extends TestCase
{
    public function test_writing_to_file() : void
    {
        $path = \sys_get_temp_dir() . '/file.snappy.parquet';
        $this->removeFile($path);

        (new Flow())
            ->read(From::rows($rows = $this->createRows(10)))
            ->write(Parquet::to($path))
            ->run();

        $this->assertEquals(
            $rows,
            (new Flow())
                ->read(Parquet::from($path))
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

        $dataFrame = (new Flow())
            ->read(From::rows($rows = new Rows(
                $this->createRow(1, new \DateTimeImmutable('2020-01-01 00:01:00')),
                $this->createRow(1, new \DateTimeImmutable('2020-01-01 00:02:00')),
                $this->createRow(1, new \DateTimeImmutable('2020-01-02 00:01:00')),
                $this->createRow(1, new \DateTimeImmutable('2020-01-02 00:02:00')),
                $this->createRow(1, new \DateTimeImmutable('2020-01-03 00:01:00')),
            )))
            ->withEntry('date', ref('datetime')->toDate(\DateTimeInterface::RFC3339)->dateFormat())
            ->partitionBy(ref('date'))
            ->write(Parquet::to($path));

        $dataFrame->run();

        $this->assertEquals(
            $rows,
            (new Flow())
                ->read(Parquet::from($path))
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
            Entry::integer('integer', $index),
            Entry::float('float', 1.5),
            Entry::string('string', 'name_' . $index),
            Entry::boolean('boolean', true),
            Entry::datetime('datetime', $dateTime ?: new \DateTimeImmutable()),
            Entry::json_object('json_object', ['id' => 1, 'name' => 'test']),
            Entry::json('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
            Entry::list_of_string('list_of_strings', ['a', 'b', 'c']),
            Entry::list_of_datetime('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()]),
            Entry::structure(
                'address',
                [
                    'street' => 'street_' . $index,
                    'city' => 'city_' . $index,
                    'zip' => 'zip_' . $index,
                    'country' => 'country_' . $index,
                    'location' => ['lat' => 1.5, 'lon' => 1.5],
                ],
                new StructureType(
                    new StructureElement('street', ScalarType::string()),
                    new StructureElement('city', ScalarType::string()),
                    new StructureElement('zip', ScalarType::string()),
                    new StructureElement('country', ScalarType::string()),
                    new StructureElement(
                        'location',
                        new StructureType(
                            new StructureElement('lat', ScalarType::float()),
                            new StructureElement('lon', ScalarType::float()),
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
