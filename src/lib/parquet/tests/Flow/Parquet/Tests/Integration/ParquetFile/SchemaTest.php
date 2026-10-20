<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\ParquetFile;

use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\Dremel\DremelAssembler;
use Flow\Parquet\Dremel\DremelShredder;
use Flow\Parquet\Dremel\ReadColumnData;
use Flow\Parquet\Dremel\Validator\ColumnDataValidator;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Tests\Context\DremelPaperDocuments;
use PHPUnit\Framework\TestCase;

use function iterator_to_array;

final class SchemaTest extends TestCase
{
    public function test_dremel_paper_data_schema(): void
    {
        $schema = DremelPaperDocuments::schema();

        $rows = DremelPaperDocuments::rows();
        $shredder = new DremelShredder(
            new ColumnDataValidator(),
            $converter = DataConverter::initialize(Options::default()),
        );
        $assembler = new DremelAssembler($converter);

        foreach ($rows as $row) {
            static::assertIsArray($row);
            $shredResult = $shredder->shred($schema, [$row], 0);

            foreach ($schema->columns() as $column) {
                $readFlatValues = [];

                if ($column instanceof FlatColumn) {
                    $flatChildren = [$column];
                } elseif ($column instanceof NestedColumn) {
                    $flatChildren = $column->childrenFlat();
                } else {
                    static::fail('Unknown column type: ' . $column::class);
                }

                foreach ($flatChildren as $flatChild) {
                    $fp = $flatChild->flatPath();
                    $wfcv = $shredResult[$fp];
                    $values = $wfcv->values();
                    $readFlatValues[] = new ReadFlatColumnValues(
                        $wfcv->column,
                        (static function () use ($values) {
                            yield from $values;
                        })(),
                        $wfcv->repetitionLevels(),
                        $wfcv->definitionLevels(),
                    );
                }

                $readData = new ReadColumnData($column, $readFlatValues);
                $assembled = iterator_to_array($assembler->assemble($column, $readData));
                /** @var array<string, mixed> $firstAssembled */
                $firstAssembled = $assembled[0];
                static::assertIsArray($firstAssembled);
                static::assertEquals($row[$column->name()], $firstAssembled[$column->name()]);
            }
        }
    }
}
