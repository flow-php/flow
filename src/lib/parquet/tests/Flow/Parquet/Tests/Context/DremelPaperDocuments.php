<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\Repetition;

/**
 * The two sample records of the Dremel paper and their schema.
 */
final class DremelPaperDocuments
{
    public static function schema(): Schema
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

    /**
     * @return list<array<string, mixed>>
     */
    public static function rows(): array
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
