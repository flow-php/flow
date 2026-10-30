<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Functions\String\StringStyles;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Values\Schema\Metadata;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rename_map;
use function Flow\ETL\DSL\rename_replace;
use function Flow\ETL\DSL\rename_style;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_json;
use function iterator_to_array;

final class RenameTest extends FlowIntegrationTestCase
{
    public function test_rename(): void
    {
        static::assertEquals(
            array_to_rows(
                [
                    ['id' => 1, 'new_name' => 'foo', 'active' => true],
                    ['id' => 2, 'new_name' => null, 'active' => false],
                    ['id' => 2, 'new_name' => 'bar', 'active' => false],
                ],
                schema(int_schema('id'), str_schema('new_name', nullable: true), bool_schema('active')),
            ),
            df()
                ->read(from_rows(array_to_rows(
                    [
                        ['id' => 1, 'name' => 'foo', 'active' => true],
                        ['id' => 2, 'name' => null, 'active' => false],
                        ['id' => 2, 'name' => 'bar', 'active' => false],
                    ],
                    schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')),
                )))
                ->rename('name', 'new_name')
                ->fetch(),
        );
    }

    public function test_rename_each_with_numeric_entry_names(): void
    {
        static::assertEquals(
            array_to_rows(
                [['broker' => 'acme', 'y_2023' => 1, 'y_2024' => 2]],
                schema(str_schema('broker'), int_schema('y_2023'), int_schema('y_2024')),
            ),
            df()
                ->read(from_rows(array_to_rows(
                    [['broker' => 'acme', '2023' => 1, '2024' => 2]],
                    schema(str_schema('broker'), int_schema('2023'), int_schema('2024')),
                )))
                ->renameEach(rename_map(['2023' => 'y_2023', '2024' => 'y_2024']))
                ->fetch(),
        );
    }

    public function test_rename_all(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'name' => 'name', 'active' => true],
                ['id' => 2, 'name' => 'name', 'active' => false],
            ],
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows([
                        ['array' => type_json()->cast(['id' => 1, 'name' => 'name', 'active' => true])],
                        ['array' => type_json()->cast(['id' => 2, 'name' => 'name', 'active' => false])],
                    ], schema(json_schema('array')))))
                    ->withColumn(
                        'row',
                        ref('array')->unpack(schema(int_schema('id'), str_schema('name'), bool_schema('active'))),
                    )
                    ->renameEach(rename_replace('row.', ''))
                    ->drop('array')
                    ->getEachAsArray(),
            ),
        );
    }

    public function test_rename_all_lower_case(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'name' => 'name', 'active' => true],
                ['id' => 2, 'name' => 'name', 'active' => false],
            ],
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows(
                        [
                            ['ID' => 1, 'NAME' => 'name', 'ACTIVE' => true],
                            ['ID' => 2, 'NAME' => 'name', 'ACTIVE' => false],
                        ],
                        schema(int_schema('ID'), str_schema('NAME'), bool_schema('ACTIVE')),
                    )))
                    ->renameEach(rename_style(StringStyles::LOWER))
                    ->getEachAsArray(),
            ),
        );
    }

    public function test_rename_all_lower_case_i18n(): void
    {
        static::assertEquals(
            [
                ['ilość przedmiotów' => 0],
                ['ilość przedmiotów' => 10],
            ],
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows([
                        ['ILOŚĆ PRZEDMIOTÓW' => 0],
                        ['ILOŚĆ PRZEDMIOTÓW' => 10],
                    ], schema(int_schema('ILOŚĆ PRZEDMIOTÓW')))))
                    ->renameEach(rename_style(StringStyles::LOWER))
                    ->getEachAsArray(),
            ),
        );
    }

    public function test_rename_all_multiple(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'name' => 'name', 'active' => true],
                ['id' => 2, 'name' => 'name', 'active' => false],
            ],
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows([
                        ['array' => type_json()->cast(['id' => 1, 'name' => 'name', 'isActive' => true])],
                        ['array' => type_json()->cast(['id' => 2, 'name' => 'name', 'isActive' => false])],
                    ], schema(json_schema('array')))))
                    ->withColumn(
                        'row',
                        ref('array')->unpack(schema(int_schema('id'), str_schema('name'), bool_schema('isActive'))),
                    )
                    ->renameEach(rename_replace(['row.', 'isActive'], ['', 'active']))
                    ->drop('array')
                    ->getEachAsArray(),
            ),
        );
    }

    public function test_rename_all_to_ascii(): void
    {
        static::assertEquals(
            [
                ['OSMY' => 8, 'DZIEWIATY' => null],
                ['OSMY' => null, 'DZIEWIATY' => 9],
            ],
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows(
                        [['ÓSMY' => 8], ['DZIEWIĄTY' => 9]],
                        schema(int_schema('ÓSMY', nullable: true), int_schema('DZIEWIĄTY', nullable: true)),
                    )))
                    ->renameEach(rename_style(StringStyles::ASCII))
                    ->getEachAsArray(),
            ),
        );
    }

    public function test_rename_all_to_camel(): void
    {
        static::assertEquals(
            [
                ['ósmyIDziewiąty' => 89],
            ],
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows([[
                        'ósmy i dziewiąty' => 89,
                    ]], schema(int_schema('ósmy i dziewiąty')))))
                    ->renameEach(rename_style(StringStyles::CAMEL))
                    ->getEachAsArray(),
            ),
        );
    }

    public function test_rename_all_to_slug(): void
    {
        static::assertEquals(
            [
                ['osmy-i-dziewiaty' => 89],
            ],
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows([[
                        'ÓSMY I DZIEWIĄTY' => 89,
                    ]], schema(int_schema('ÓSMY I DZIEWIĄTY')))))
                    ->renameEach(rename_style(StringStyles::SLUG))
                    ->renameEach(rename_style(StringStyles::LOWER))
                    ->getEachAsArray(),
            ),
        );
    }

    public function test_rename_all_to_snake_case(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'user_name' => 'name', 'is_active' => true],
                ['id' => 2, 'user_name' => 'name', 'is_active' => false],
            ],
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows(
                        [
                            ['id' => 1, 'UserName' => 'name', 'isActive' => true],
                            ['id' => 2, 'UserName' => 'name', 'isActive' => false],
                        ],
                        schema(int_schema('id'), str_schema('UserName'), bool_schema('isActive')),
                    )))
                    ->renameEach(rename_style(StringStyles::SNAKE))
                    ->renameEach(rename_style(StringStyles::LOWER))
                    ->getEachAsArray(),
            ),
        );
    }

    public function test_rename_all_to_title(): void
    {
        static::assertEquals(
            [
                ['Ósmy i dziewiąty' => 89],
            ],
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows([[
                        'ósmy i dziewiąty' => 89,
                    ]], schema(int_schema('ósmy i dziewiąty')))))
                    ->renameEach(rename_style(StringStyles::TITLE))
                    ->getEachAsArray(),
            ),
        );
    }

    public function test_rename_all_upper_case(): void
    {
        static::assertEquals(
            [
                ['ID' => 1, 'NAME' => 'name', 'ACTIVE' => true],
                ['ID' => 2, 'NAME' => 'name', 'ACTIVE' => false],
            ],
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows(
                        [
                            ['id' => 1, 'name' => 'name', 'active' => true],
                            ['id' => 2, 'name' => 'name', 'active' => false],
                        ],
                        schema(int_schema('id'), str_schema('name'), bool_schema('active')),
                    )))
                    ->renameEach(rename_style(StringStyles::UPPER))
                    ->getEachAsArray(),
            ),
        );
    }

    public function test_rename_all_upper_case_first(): void
    {
        static::assertEquals(
            [
                ['Id' => 1, 'Name' => 'name', 'Active' => true],
                ['Id' => 2, 'Name' => 'name', 'Active' => false],
            ],
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows(
                        [
                            ['id' => 1, 'name' => 'name', 'active' => true],
                            ['id' => 2, 'name' => 'name', 'active' => false],
                        ],
                        schema(int_schema('id'), str_schema('name'), bool_schema('active')),
                    )))
                    ->renameEach(rename_style(StringStyles::UCFIRST))
                    ->getEachAsArray(),
            ),
        );
    }

    public function test_rename_all_upper_case_word(): void
    {
        static::assertEquals(
            [
                ['Id' => 1, 'Name' => 'name', 'Active' => true],
                ['Id' => 2, 'Name' => 'name', 'Active' => false],
            ],
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows(
                        [
                            ['id' => 1, 'name' => 'name', 'active' => true],
                            ['id' => 2, 'name' => 'name', 'active' => false],
                        ],
                        schema(int_schema('id'), str_schema('name'), bool_schema('active')),
                    )))
                    ->renameEach(rename_style(StringStyles::UCWORDS))
                    ->getEachAsArray(),
            ),
        );
    }

    public function test_rename_all_upper_case_word_i18n(): void
    {
        static::assertEquals(
            [
                ['Ósmy' => 8, 'Dziewiąty' => null],
                ['Ósmy' => null, 'Dziewiąty' => 9],
            ],
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows(
                        [['ósmy' => 8], ['dziewiąty' => 9]],
                        schema(int_schema('ósmy', nullable: true), int_schema('dziewiąty', nullable: true)),
                    )))
                    ->renameEach(rename_style(StringStyles::UCWORDS))
                    ->getEachAsArray(),
            ),
        );
    }

    public function test_rename_each_with_empty_map(): void
    {
        static::assertEquals(
            array_to_rows([['id' => 1, 'name' => 'foo']], schema(int_schema('id'), str_schema('name'))),
            df()
                ->read(from_rows(array_to_rows(
                    [['id' => 1, 'name' => 'foo']],
                    schema(int_schema('id'), str_schema('name')),
                )))
                ->renameEach(rename_map([]))
                ->fetch(),
        );
    }

    public function test_rename_each_with_map_chained_with_other_operations(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'name' => 'John'],
            ],
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows(
                        [
                            ['user_id' => 1, 'user_name' => 'John', 'is_active' => true],
                            ['user_id' => 2, 'user_name' => 'Jane', 'is_active' => false],
                        ],
                        schema(int_schema('user_id'), str_schema('user_name'), bool_schema('is_active')),
                    )))
                    ->renameEach(rename_map(['user_id' => 'id', 'user_name' => 'name']))
                    ->filter(ref('is_active')->equals(lit(true)))
                    ->drop('is_active')
                    ->getEachAsArray(),
            ),
        );
    }

    public function test_rename_each_with_map_multiple_entries(): void
    {
        static::assertEquals(
            array_to_rows(
                [
                    ['id' => 1, 'name' => 'John', 'surname' => 'Doe'],
                    ['id' => 2, 'name' => 'Jane', 'surname' => 'Smith'],
                ],
                schema(int_schema('id'), str_schema('name'), str_schema('surname')),
            ),
            df()
                ->read(from_rows(array_to_rows(
                    [
                        ['id' => 1, 'first_name' => 'John', 'last_name' => 'Doe'],
                        ['id' => 2, 'first_name' => 'Jane', 'last_name' => 'Smith'],
                    ],
                    schema(int_schema('id'), str_schema('first_name'), str_schema('last_name')),
                )))
                ->renameEach(rename_map([
                    'first_name' => 'name',
                    'last_name' => 'surname',
                ]))
                ->fetch(),
        );
    }

    public function test_rename_each_with_map_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata']);

        static::assertTrue(
            df()
                ->read(from_rows(array_to_rows([[
                    'old_name' => 'value',
                ]], schema(str_schema('old_name', metadata: $metadata)))))
                ->renameEach(rename_map(['old_name' => 'new_name']))
                ->fetch()
                ->schema()
                ->get('new_name')
                ->metadata()
                ->isEqual($metadata),
        );
    }

    public function test_rename_each_with_multiple_strategies(): void
    {
        static::assertEquals(
            [
                ['osmy' => 8, 'dziewiaty' => null, 'osmy-i-dziewiaty' => null],
                ['osmy' => null, 'dziewiaty' => 9, 'osmy-i-dziewiaty' => null],
                ['osmy' => null, 'dziewiaty' => null, 'osmy-i-dziewiaty' => 89],
            ],
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows(
                        [['ÓSMY' => 8], ['DZIEWIĄTY' => 9], ['ÓSMY I DZIEWIĄTY' => 89]],
                        schema(
                            int_schema('ÓSMY', nullable: true),
                            int_schema('DZIEWIĄTY', nullable: true),
                            int_schema('ÓSMY I DZIEWIĄTY', nullable: true),
                        ),
                    )))
                    ->renameEach(
                        rename_style(StringStyles::ASCII),
                        rename_style(StringStyles::LOWER),
                        rename_style(StringStyles::KEBAB),
                    )
                    ->getEachAsArray(),
            ),
        );
    }

    public function test_rename_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata']);

        static::assertTrue(
            df()
                ->read(from_rows(array_to_rows([[
                    'old_name' => 'value',
                ]], schema(str_schema('old_name', metadata: $metadata)))))
                ->rename('old_name', 'new_name')
                ->fetch()
                ->schema()
                ->get('new_name')
                ->metadata()
                ->isEqual($metadata),
        );
    }
}
