<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\ParquetFile;

use Flow\Dremel\{DataShredded, Dremel};
use Flow\Parquet\ParquetFile\RowGroupBuilder\Validator\DisabledValidator;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn, Repetition};
use PHPUnit\Framework\TestCase;

final class SchemaTest extends TestCase
{
    public function test_shredding_and_assembling_doc_id() : void
    {
        self::markTestSkipped();
        $data = [
            $this->flattener()->flattenRow($this->dremelPaperDataSchema()->get('DocId'), $this->dremelPaperDataStructure()[0]),
            $this->flattener()->flattenRow($this->dremelPaperDataSchema()->get('DocId'), $this->dremelPaperDataStructure()[1]),
        ];

        self::assertSame([['DocId' => 10], ['DocId' => 20]], $data);

        self::assertEquals(
            new DataShredded(
                repetitionLevels: [0, 0],
                definitionLevels: [0, 0],
                values: [10, 20]
            ),
            $shredded = (new Dremel())->shred([$data[0]['DocId'], $data[1]['DocId']], $this->dremelPaperDataSchema()->get('DocId')->maxDefinitionsLevel())
        );
        self::assertEquals(
            [$data[0]['DocId'], $data[1]['DocId']],
            (new Dremel())->assemble(
                $shredded->repetitionLevels,
                $shredded->definitionLevels,
                $shredded->values,
                $this->dremelPaperDataSchema()->get('DocId')->maxDefinitionsLevel(),
                $this->dremelPaperDataSchema()->get('DocId')->maxRepetitionsLevel(),
                $this->dremelPaperDataSchema()->get('DocId')->repetition() === Repetition::REQUIRED
            )
        );
    }

    public function test_shredding_and_assembling_links_backward() : void
    {
        self::markTestSkipped();
        $data = [
            $this->flattener()->flattenRow($this->dremelPaperDataSchema()->get('Links'), $this->dremelPaperDataStructure()[0]),
            $this->flattener()->flattenRow($this->dremelPaperDataSchema()->get('Links'), $this->dremelPaperDataStructure()[1]),
        ];

        self::assertSame([['Backward' => null], ['Backward' => [10, 30]]], $data);

        //        self::assertEquals(
        //            new DataShredded(
        //                repetitions: [0, 0],
        //                definitions: [0, 0],
        //                values: [10, 30]
        //            ),
        //            $shredded = (new Dremel())->shred([$data[0]['DocId'], $data[1]['DocId']], $this->dremelPaperDataSchema()->get('DocId')->maxDefinitionsLevel())
        //        );
        //        self::assertEquals(
        //            [$data[0]['DocId'], $data[1]['DocId']],
        //            (new Dremel())->assemble(
        //                $shredded->repetitions,
        //                $shredded->definitions,
        //                $shredded->values,
        //                $this->dremelPaperDataSchema()->get('DocId')->maxDefinitionsLevel(),
        //                $this->dremelPaperDataSchema()->get('DocId')->maxRepetitionsLevel(),
        //                $this->dremelPaperDataSchema()->get('DocId')->repetition() === Repetition::REQUIRED
        //            )
        //        );
    }

    private function dremelPaperDataSchema() : Schema
    {
        return Schema::with(
            FlatColumn::int32('DocId', Repetition::REQUIRED),
            NestedColumn::list(
                'Links',
                ListElement::structure([
                    FlatColumn::int32('Backward', Repetition::REQUIRED),
                    FlatColumn::int32('Forward', Repetition::REQUIRED),
                ])
            ),
            NestedColumn::list(
                'Name',
                ListElement::structure([
                    NestedColumn::list(
                        'Language',
                        ListElement::structure([
                            FlatColumn::string('Code'),
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
                    ],
                ],
                'Name' => [
                    [
                        'Language' => [
                            ['Code' => 'en-us', 'Country' => 'us'],
                            ['Code' => 'en'],
                        ],
                        'Url' => 'http://A',
                    ],
                    [
                        'Url' => 'http://B',
                    ],
                    [
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
                    ],
                ],
            ],
        ];
    }

    private function flattener() : Dremel
    {
        return new Dremel(new DisabledValidator());
    }
}
