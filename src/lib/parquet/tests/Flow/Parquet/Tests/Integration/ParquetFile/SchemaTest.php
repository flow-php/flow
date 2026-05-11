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
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use PHPUnit\Framework\TestCase;

final class SchemaTest extends TestCase
{
    public function test_dremel_paper_data_schema(): void
    {
        $schema = $this->dremelPaperDataSchema();

        $rows = $this->dremelPaperDataStructure();
        $shredder = new DremelShredder(
            new ColumnDataValidator(),
            $converter = DataConverter::initialize(Options::default()),
        );
        $assembler = new DremelAssembler($converter);

        foreach ($rows as $row) {
            $shredResult = $shredder->shred($schema, [$row]);

            foreach ($schema->columns() as $column) {
                $readFlatValues = [];

                $flatChildren = $column instanceof FlatColumn ? [$column] : $column->childrenFlat();

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
                static::assertEquals(
                    $row[$column->name()],
                    \iterator_to_array($assembler->assemble($column, $readData))[0][$column->name()],
                );
            }
        }
    }

    private function dremelPaperDataSchema(): Schema
    {
        return Schema::with(
            FlatColumn::int32('DocId', Repetition::REQUIRED),
            NestedColumn::list('Links', ListElement::structure([
                NestedColumn::list('Backward', ListElement::int32()),
                NestedColumn::list('Forward', ListElement::int32()),
            ])),
            NestedColumn::list(
                'Name',
                ListElement::structure([
                    NestedColumn::list(
                        'Language',
                        ListElement::structure([
                            FlatColumn::string('Code', Repetition::REQUIRED),
                            FlatColumn::string('Country'),
                        ]),
                        Repetition::OPTIONAL,
                    ),
                    FlatColumn::string('Url', Repetition::OPTIONAL),
                ]),
                Repetition::OPTIONAL,
            ),
        );
    }

    private function dremelPaperDataStructure(): array
    {
        return [
            [
                'DocId' => 10,
                'Links' => [
                    [
                        'Forward' => [20, 40, 60],
                        'Backward' => null,
                    ],
                ],
                'Name' => [
                    [
                        'Url' => 'http://A',
                        'Language' => [
                            ['Code' => 'en-us', 'Country' => 'us'],
                            ['Code' => 'en', 'Country' => null],
                        ],
                    ],
                    [
                        'Url' => 'http://B',
                        'Language' => null,
                    ],
                    [
                        'Url' => null,
                        'Language' => [
                            ['Code' => 'en-gb', 'Country' => 'gb'],
                        ],
                    ],
                ],
            ],
            [
                'DocId' => 20,
                'Links' => [
                    [
                        'Backward' => [10, 30],
                        'Forward' => [80],
                    ],
                ],
                'Name' => [
                    [
                        'Url' => 'http://C',
                        'Language' => null,
                    ],
                ],
            ],
        ];
    }
}
