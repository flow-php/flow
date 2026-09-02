<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Schema\Metadata;
use Flow\ETL\String\StringStyles;
use Flow\ETL\Tests\FlowIntegrationTestCase;

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
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_json;
use function iterator_to_array;

final class RenameTest extends FlowIntegrationTestCase
{
    public function test_rename(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')),
                row(['id' => 1, 'name' => 'foo', 'active' => true]),
                row(['id' => 2, 'name' => null, 'active' => false]),
                row(['id' => 2, 'name' => 'bar', 'active' => false]),
            )))
            ->rename('name', 'new_name')
            ->fetch();

        static::assertEquals(
            rows(
                schema(int_schema('id'), str_schema('new_name', nullable: true), bool_schema('active')),
                row(['id' => 1, 'new_name' => 'foo', 'active' => true]),
                row(['id' => 2, 'new_name' => null, 'active' => false]),
                row(['id' => 2, 'new_name' => 'bar', 'active' => false]),
            ),
            $rows,
        );
    }

    public function test_rename_all(): void
    {
        $rows = rows(
            schema(json_schema('array')),
            row(['array' => type_json()->cast(['id' => 1, 'name' => 'name', 'active' => true])]),
            row(['array' => type_json()->cast(['id' => 2, 'name' => 'name', 'active' => false])]),
        );

        $ds = df()
            ->read(from_rows($rows))
            ->withEntry('row', ref('array')->unpack())
            ->renameEach(rename_replace('row.', ''))
            ->drop('array')
            ->getEachAsArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'name', 'active' => true],
                ['id' => 2, 'name' => 'name', 'active' => false],
            ],
            iterator_to_array($ds),
        );
    }

    public function test_rename_all_lower_case(): void
    {
        $rows = rows(
            schema(int_schema('ID'), str_schema('NAME'), bool_schema('ACTIVE')),
            row(['ID' => 1, 'NAME' => 'name', 'ACTIVE' => true]),
            row(['ID' => 2, 'NAME' => 'name', 'ACTIVE' => false]),
        );

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::LOWER))->getEachAsArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'name', 'active' => true],
                ['id' => 2, 'name' => 'name', 'active' => false],
            ],
            iterator_to_array($ds),
        );
    }

    public function test_rename_all_lower_case_i18n(): void
    {
        $rows = rows(
            schema(int_schema('ILOŚĆ PRZEDMIOTÓW')),
            row(['ILOŚĆ PRZEDMIOTÓW' => 0]),
            row(['ILOŚĆ PRZEDMIOTÓW' => 10]),
        );

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::LOWER))->getEachAsArray();

        static::assertEquals(
            [
                ['ilość przedmiotów' => 0],
                ['ilość przedmiotów' => 10],
            ],
            iterator_to_array($ds),
        );
    }

    public function test_rename_all_multiple(): void
    {
        $rows = rows(
            schema(json_schema('array')),
            row(['array' => type_json()->cast(['id' => 1, 'name' => 'name', 'isActive' => true])]),
            row(['array' => type_json()->cast(['id' => 2, 'name' => 'name', 'isActive' => false])]),
        );

        $ds = df()
            ->read(from_rows($rows))
            ->withEntry('row', ref('array')->unpack())
            ->renameEach(rename_replace(['row.', 'isActive'], ['', 'active']))
            ->drop('array')
            ->getEachAsArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'name', 'active' => true],
                ['id' => 2, 'name' => 'name', 'active' => false],
            ],
            iterator_to_array($ds),
        );
    }

    public function test_rename_all_to_ascii(): void
    {
        $rows = rows(
            schema(int_schema('ÓSMY', nullable: true), int_schema('DZIEWIĄTY', nullable: true)),
            row(['ÓSMY' => 8]),
            row(['DZIEWIĄTY' => 9]),
        );

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::ASCII))->getEachAsArray();

        static::assertEquals(
            [
                ['OSMY' => 8, 'DZIEWIATY' => null],
                ['OSMY' => null, 'DZIEWIATY' => 9],
            ],
            iterator_to_array($ds),
        );
    }

    public function test_rename_all_to_camel(): void
    {
        $rows = rows(schema(int_schema('ósmy i dziewiąty')), row(['ósmy i dziewiąty' => 89]));

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::CAMEL))->getEachAsArray();

        static::assertEquals(
            [
                ['ósmyIDziewiąty' => 89],
            ],
            iterator_to_array($ds),
        );
    }

    public function test_rename_all_to_slug(): void
    {
        $rows = rows(schema(int_schema('ÓSMY I DZIEWIĄTY')), row(['ÓSMY I DZIEWIĄTY' => 89]));

        $ds = df()
            ->read(from_rows($rows))
            ->renameEach(rename_style(StringStyles::SLUG))
            ->renameEach(rename_style(StringStyles::LOWER))
            ->getEachAsArray();

        static::assertEquals(
            [
                ['osmy-i-dziewiaty' => 89],
            ],
            iterator_to_array($ds),
        );
    }

    public function test_rename_all_to_snake_case(): void
    {
        $rows = rows(
            schema(int_schema('id'), str_schema('UserName'), bool_schema('isActive')),
            row(['id' => 1, 'UserName' => 'name', 'isActive' => true]),
            row(['id' => 2, 'UserName' => 'name', 'isActive' => false]),
        );

        $ds = df()
            ->read(from_rows($rows))
            ->renameEach(rename_style(StringStyles::SNAKE))
            ->renameEach(rename_style(StringStyles::LOWER))
            ->getEachAsArray();

        static::assertEquals(
            [
                ['id' => 1, 'user_name' => 'name', 'is_active' => true],
                ['id' => 2, 'user_name' => 'name', 'is_active' => false],
            ],
            iterator_to_array($ds),
        );
    }

    public function test_rename_all_to_title(): void
    {
        $rows = rows(schema(int_schema('ósmy i dziewiąty')), row(['ósmy i dziewiąty' => 89]));

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::TITLE))->getEachAsArray();

        static::assertEquals(
            [
                ['Ósmy i dziewiąty' => 89],
            ],
            iterator_to_array($ds),
        );
    }

    public function test_rename_all_upper_case(): void
    {
        $rows = rows(
            schema(int_schema('id'), str_schema('name'), bool_schema('active')),
            row(['id' => 1, 'name' => 'name', 'active' => true]),
            row(['id' => 2, 'name' => 'name', 'active' => false]),
        );

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::UPPER))->getEachAsArray();

        static::assertEquals(
            [
                ['ID' => 1, 'NAME' => 'name', 'ACTIVE' => true],
                ['ID' => 2, 'NAME' => 'name', 'ACTIVE' => false],
            ],
            iterator_to_array($ds),
        );
    }

    public function test_rename_all_upper_case_first(): void
    {
        $rows = rows(
            schema(int_schema('id'), str_schema('name'), bool_schema('active')),
            row(['id' => 1, 'name' => 'name', 'active' => true]),
            row(['id' => 2, 'name' => 'name', 'active' => false]),
        );

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::UCFIRST))->getEachAsArray();

        static::assertEquals(
            [
                ['Id' => 1, 'Name' => 'name', 'Active' => true],
                ['Id' => 2, 'Name' => 'name', 'Active' => false],
            ],
            iterator_to_array($ds),
        );
    }

    public function test_rename_all_upper_case_word(): void
    {
        $rows = rows(
            schema(int_schema('id'), str_schema('name'), bool_schema('active')),
            row(['id' => 1, 'name' => 'name', 'active' => true]),
            row(['id' => 2, 'name' => 'name', 'active' => false]),
        );

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::UCWORDS))->getEachAsArray();

        static::assertEquals(
            [
                ['Id' => 1, 'Name' => 'name', 'Active' => true],
                ['Id' => 2, 'Name' => 'name', 'Active' => false],
            ],
            iterator_to_array($ds),
        );
    }

    public function test_rename_all_upper_case_word_i18n(): void
    {
        $rows = rows(
            schema(int_schema('ósmy', nullable: true), int_schema('dziewiąty', nullable: true)),
            row(['ósmy' => 8]),
            row(['dziewiąty' => 9]),
        );

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::UCWORDS))->getEachAsArray();

        static::assertEquals(
            [
                ['Ósmy' => 8, 'Dziewiąty' => null],
                ['Ósmy' => null, 'Dziewiąty' => 9],
            ],
            iterator_to_array($ds),
        );
    }

    public function test_rename_each_with_empty_map(): void
    {
        $rows = df()
            ->read(from_rows(rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'foo']))))
            ->renameEach(rename_map([]))
            ->fetch();

        static::assertEquals(
            rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'foo'])),
            $rows,
        );
    }

    public function test_rename_each_with_map_chained_with_other_operations(): void
    {
        $ds = df()
            ->read(from_rows(rows(
                schema(int_schema('user_id'), str_schema('user_name'), bool_schema('is_active')),
                row(['user_id' => 1, 'user_name' => 'John', 'is_active' => true]),
                row(['user_id' => 2, 'user_name' => 'Jane', 'is_active' => false]),
            )))
            ->renameEach(rename_map(['user_id' => 'id', 'user_name' => 'name']))
            ->filter(ref('is_active')->equals(lit(true)))
            ->drop('is_active')
            ->getEachAsArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'John'],
            ],
            iterator_to_array($ds),
        );
    }

    public function test_rename_each_with_map_multiple_entries(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                schema(int_schema('id'), str_schema('first_name'), str_schema('last_name')),
                row(['id' => 1, 'first_name' => 'John', 'last_name' => 'Doe']),
                row(['id' => 2, 'first_name' => 'Jane', 'last_name' => 'Smith']),
            )))
            ->renameEach(rename_map([
                'first_name' => 'name',
                'last_name' => 'surname',
            ]))
            ->fetch();

        static::assertEquals(
            rows(
                schema(int_schema('id'), str_schema('name'), str_schema('surname')),
                row(['id' => 1, 'name' => 'John', 'surname' => 'Doe']),
                row(['id' => 2, 'name' => 'Jane', 'surname' => 'Smith']),
            ),
            $rows,
        );
    }

    public function test_rename_each_with_map_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata']);

        $rows = df()
            ->read(from_rows(rows(schema(str_schema('old_name', metadata: $metadata)), row(['old_name' => 'value']))))
            ->renameEach(rename_map(['old_name' => 'new_name']))
            ->fetch();

        static::assertTrue($rows->schema()->get('new_name')->metadata()->isEqual($metadata));
    }

    public function test_rename_each_with_multiple_strategies(): void
    {
        $rows = rows(
            schema(
                int_schema('ÓSMY', nullable: true),
                int_schema('DZIEWIĄTY', nullable: true),
                int_schema('ÓSMY I DZIEWIĄTY', nullable: true),
            ),
            row(['ÓSMY' => 8]),
            row(['DZIEWIĄTY' => 9]),
            row(['ÓSMY I DZIEWIĄTY' => 89]),
        );

        $ds = df()
            ->read(from_rows($rows))
            ->renameEach(
                rename_style(StringStyles::ASCII),
                rename_style(StringStyles::LOWER),
                rename_style(StringStyles::KEBAB),
            )
            ->getEachAsArray();

        static::assertEquals(
            [
                ['osmy' => 8, 'dziewiaty' => null, 'osmy-i-dziewiaty' => null],
                ['osmy' => null, 'dziewiaty' => 9, 'osmy-i-dziewiaty' => null],
                ['osmy' => null, 'dziewiaty' => null, 'osmy-i-dziewiaty' => 89],
            ],
            iterator_to_array($ds),
        );
    }

    public function test_rename_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata']);

        $rows = df()
            ->read(from_rows(rows(schema(str_schema('old_name', metadata: $metadata)), row(['old_name' => 'value']))))
            ->rename('old_name', 'new_name')
            ->fetch();

        static::assertTrue($rows->schema()->get('new_name')->metadata()->isEqual($metadata));
    }
}
