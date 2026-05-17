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

        // @mago-ignore analysis:mixed-assignment
        foreach ($rows as $row) {
            $narrowedRow = \Flow\Types\DSL\type_map(
                \Flow\Types\DSL\type_string(),
                \Flow\Types\DSL\type_mixed(),
            )->assert($row);
            $shredResult = $shredder->shred($schema, [$narrowedRow]);

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
                $assembled = \iterator_to_array($assembler->assemble($column, $readData));
                $firstAssembled = \Flow\Types\DSL\type_array()->assert($assembled[0]);
                static::assertEquals($narrowedRow[$column->name()], $firstAssembled[$column->name()]);
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
