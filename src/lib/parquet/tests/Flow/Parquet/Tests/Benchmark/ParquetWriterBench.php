<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Benchmark;

use Flow\Parquet\{Options, Writer};
use Flow\Parquet\ParquetFile\{Compressions, Schema};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, MapKey, MapValue, NestedColumn};
use PhpBench\Attributes\Groups;

#[Groups(['parquet-library'])]
final class ParquetWriterBench
{
    private readonly string $outputPath;

    private array $rows;

    private readonly Schema $schema;

    public function __construct()
    {
        $this->schema = Schema::with(
            FlatColumn::int32('id'),
            FlatColumn::string('name'),
            FlatColumn::double('price'),
            FlatColumn::float('rating'),
            FlatColumn::boolean('active'),
            FlatColumn::int64('timestamp'),
            FlatColumn::date('created_date'),
            FlatColumn::dateTime('last_modified'),
            FlatColumn::time('daily_time'),
            FlatColumn::decimal('amount', 10, 2),
            FlatColumn::json('metadata'),
            FlatColumn::uuid('uuid'),
            NestedColumn::list('tags', ListElement::string()),
            NestedColumn::map('properties', MapKey::string(), MapValue::string()),
            NestedColumn::create('address', [
                FlatColumn::string('street'),
                FlatColumn::string('city'),
                FlatColumn::string('country'),
                NestedColumn::create('coordinates', [
                    FlatColumn::double('lat'),
                    FlatColumn::double('lng'),
                ]),
            ])
        );

        $this->rows = [];

        for ($i = 1; $i <= 1000; $i++) {
            $this->rows[] = [
                'id' => $i,
                'name' => 'Item ' . $i,
                'price' => $i * 1.5,
                'rating' => 4.5 + ($i % 5) * 0.1,
                'active' => $i % 2 === 0,
                'timestamp' => 1640995200000000 + $i * 3600000000,
                'created_date' => new \DateTimeImmutable('2022-01-01'),
                'last_modified' => new \DateTimeImmutable('2022-01-0' . (($i % 9) + 1)),
                'daily_time' => new \DateInterval('PT12H30M45S'),
                'amount' => 100.50 + ($i * 0.1),
                'metadata' => \json_encode(['category' => 'type_' . ($i % 3), 'version' => $i]),
                'uuid' => \sprintf('%08x-%04x-%04x-%04x-%012x', $i, $i, $i, $i, $i),
                'tags' => ['tag_' . ($i % 5), 'category_' . ($i % 3)],
                'properties' => [
                    'color' => ['red', 'green', 'blue'][$i % 3],
                    'size' => ['small', 'medium', 'large'][$i % 3],
                ],
                'address' => [
                    'street' => 'Street ' . $i,
                    'city' => 'City ' . ($i % 10),
                    'country' => ['US', 'UK', 'CA'][$i % 3],
                    'coordinates' => [
                        'lat' => 40.7128 + ($i % 100) * 0.01,
                        'lng' => -74.0060 + ($i % 100) * 0.01,
                    ],
                ],
            ];
        }

        $this->outputPath = \tempnam(\sys_get_temp_dir(), 'parquet_writer_bench') . '.parquet';
    }

    public function __destruct()
    {
        if (\file_exists($this->outputPath)) {
            \unlink($this->outputPath);
        }
    }

    public function bench_write_batch() : void
    {
        $writer = new Writer(Compressions::SNAPPY, new Options());
        $writer->open($this->outputPath, $this->schema);
        $writer->writeBatch($this->rows);
        $writer->close();
    }

    public function bench_write_gzip() : void
    {
        $writer = new Writer(Compressions::GZIP, new Options());
        $writer->write($this->outputPath, $this->schema, $this->rows);
    }

    public function bench_write_row_by_row() : void
    {
        $writer = new Writer(Compressions::SNAPPY, new Options());
        $writer->open($this->outputPath, $this->schema);

        foreach ($this->rows as $row) {
            $writer->writeRow($row);
        }

        $writer->close();
    }

    public function bench_write_snappy() : void
    {
        $writer = new Writer(Compressions::SNAPPY, new Options());
        $writer->write($this->outputPath, $this->schema, $this->rows);
    }

    public function bench_write_uncompressed() : void
    {
        $writer = new Writer(Compressions::UNCOMPRESSED, new Options());
        $writer->write($this->outputPath, $this->schema, $this->rows);
    }
}
