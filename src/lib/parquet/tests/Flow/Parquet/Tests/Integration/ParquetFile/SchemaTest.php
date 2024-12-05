<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\ParquetFile;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Validator\{ColumnDataValidator};
use Flow\Parquet\ParquetFile\RowGroupBuilder\{DremelAssembler, DremelShredder};
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn, Repetition};
use PHPUnit\Framework\TestCase;

final class SchemaTest extends TestCase
{
    public function test_dremel_paper_data_schema() : void
    {
        $schema = $this->dremelPaperDataSchema();

        $data = $this->dremelPaperDataStructure();
        $shredder = new DremelShredder(new ColumnDataValidator(), $converter = DataConverter::initialize(Options::default()));
        $assembler = new DremelAssembler($converter);

        foreach ($data as $row) {
            foreach ($schema->columns() as $column) {
                $data = $shredder->shred($column, $row);
                self::assertEquals($row[$column->name()], $assembler->assemble($column, $data)[0][$column->name()]);
            }
        }
    }

    private function dremelPaperDataSchema() : Schema
    {
        return Schema::with(
            FlatColumn::int32('DocId', Repetition::REQUIRED),
            NestedColumn::list(
                'Links',
                ListElement::structure([
                    NestedColumn::list('Backward', ListElement::int32()),
                    NestedColumn::list('Forward', ListElement::int32()),
                ])
            ),
            NestedColumn::list(
                'Name',
                ListElement::structure([
                    NestedColumn::list(
                        'Language',
                        ListElement::structure([
                            FlatColumn::string('Code', Repetition::REQUIRED),
                            FlatColumn::string('Country'),
                        ]),
                        Repetition::OPTIONAL
                    ),
                    FlatColumn::string('Url', Repetition::OPTIONAL),
                ]),
                Repetition::OPTIONAL
            )
        );
    }

    private function dremelPaperDataStructure() : array
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
