<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Schema\Metadata;
use Flow\ETL\String\StringStyles;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rename_map;
use function Flow\ETL\DSL\rename_replace;
use function Flow\ETL\DSL\rename_style;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;

final class RenameTest extends FlowIntegrationTestCase
{
    public function test_rename(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                row(int_entry('id', 2), str_entry('name', null), bool_entry('active', false)),
                row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            )))
            ->rename('name', 'new_name')
            ->fetch();

        static::assertEquals(
            rows(
                row(int_entry('id', 1), str_entry('new_name', 'foo'), bool_entry('active', true)),
                row(int_entry('id', 2), str_entry('new_name', null), bool_entry('active', false)),
                row(int_entry('id', 2), str_entry('new_name', 'bar'), bool_entry('active', false)),
            ),
            $rows,
        );
    }

    public function test_rename_all(): void
    {
        $rows = rows(
            row(json_entry('array', ['id' => 1, 'name' => 'name', 'active' => true])),
            row(json_entry('array', ['id' => 2, 'name' => 'name', 'active' => false])),
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
            \iterator_to_array($ds),
        );
    }

    public function test_rename_all_lower_case(): void
    {
        $rows = rows(
            row(int_entry('ID', 1), str_entry('NAME', 'name'), bool_entry('ACTIVE', true)),
            row(int_entry('ID', 2), str_entry('NAME', 'name'), bool_entry('ACTIVE', false)),
        );

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::LOWER))->getEachAsArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'name', 'active' => true],
                ['id' => 2, 'name' => 'name', 'active' => false],
            ],
            \iterator_to_array($ds),
        );
    }

    public function test_rename_all_lower_case_i18n(): void
    {
        $rows = rows(row(int_entry('ILOŚĆ PRZEDMIOTÓW', 0)), row(int_entry('ILOŚĆ PRZEDMIOTÓW', 10)));

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::LOWER))->getEachAsArray();

        static::assertEquals(
            [
                ['ilość przedmiotów' => 0],
                ['ilość przedmiotów' => 10],
            ],
            \iterator_to_array($ds),
        );
    }

    public function test_rename_all_multiple(): void
    {
        $rows = rows(
            row(json_entry('array', ['id' => 1, 'name' => 'name', 'isActive' => true])),
            row(json_entry('array', ['id' => 2, 'name' => 'name', 'isActive' => false])),
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
            \iterator_to_array($ds),
        );
    }

    public function test_rename_all_to_ascii(): void
    {
        $rows = rows(row(int_entry('ÓSMY', 8)), row(int_entry('DZIEWIĄTY', 9)));

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::ASCII))->getEachAsArray();

        static::assertEquals(
            [
                ['OSMY' => 8],
                ['DZIEWIATY' => 9],
            ],
            \iterator_to_array($ds),
        );
    }

    public function test_rename_all_to_camel(): void
    {
        $rows = rows(row(int_entry('ósmy i dziewiąty', 89)));

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::CAMEL))->getEachAsArray();

        static::assertEquals(
            [
                ['ósmyIDziewiąty' => 89],
            ],
            \iterator_to_array($ds),
        );
    }

    public function test_rename_all_to_slug(): void
    {
        $rows = rows(row(int_entry('ÓSMY I DZIEWIĄTY', 89)));

        $ds = df()
            ->read(from_rows($rows))
            ->renameEach(rename_style(StringStyles::SLUG))
            ->renameEach(rename_style(StringStyles::LOWER))
            ->getEachAsArray();

        static::assertEquals(
            [
                ['osmy-i-dziewiaty' => 89],
            ],
            \iterator_to_array($ds),
        );
    }

    public function test_rename_all_to_snake_case(): void
    {
        $rows = rows(
            row(int_entry('id', 1), str_entry('UserName', 'name'), bool_entry('isActive', true)),
            row(int_entry('id', 2), str_entry('UserName', 'name'), bool_entry('isActive', false)),
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
            \iterator_to_array($ds),
        );
    }

    public function test_rename_all_to_title(): void
    {
        $rows = rows(row(int_entry('ósmy i dziewiąty', 89)));

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::TITLE))->getEachAsArray();

        static::assertEquals(
            [
                ['Ósmy i dziewiąty' => 89],
            ],
            \iterator_to_array($ds),
        );
    }

    public function test_rename_all_upper_case(): void
    {
        $rows = rows(
            row(int_entry('id', 1), str_entry('name', 'name'), bool_entry('active', true)),
            row(int_entry('id', 2), str_entry('name', 'name'), bool_entry('active', false)),
        );

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::UPPER))->getEachAsArray();

        static::assertEquals(
            [
                ['ID' => 1, 'NAME' => 'name', 'ACTIVE' => true],
                ['ID' => 2, 'NAME' => 'name', 'ACTIVE' => false],
            ],
            \iterator_to_array($ds),
        );
    }

    public function test_rename_all_upper_case_first(): void
    {
        $rows = rows(
            row(int_entry('id', 1), str_entry('name', 'name'), bool_entry('active', true)),
            row(int_entry('id', 2), str_entry('name', 'name'), bool_entry('active', false)),
        );

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::UCFIRST))->getEachAsArray();

        static::assertEquals(
            [
                ['Id' => 1, 'Name' => 'name', 'Active' => true],
                ['Id' => 2, 'Name' => 'name', 'Active' => false],
            ],
            \iterator_to_array($ds),
        );
    }

    public function test_rename_all_upper_case_word(): void
    {
        $rows = rows(
            row(int_entry('id', 1), str_entry('name', 'name'), bool_entry('active', true)),
            row(int_entry('id', 2), str_entry('name', 'name'), bool_entry('active', false)),
        );

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::UCWORDS))->getEachAsArray();

        static::assertEquals(
            [
                ['Id' => 1, 'Name' => 'name', 'Active' => true],
                ['Id' => 2, 'Name' => 'name', 'Active' => false],
            ],
            \iterator_to_array($ds),
        );
    }

    public function test_rename_all_upper_case_word_i18n(): void
    {
        $rows = rows(row(int_entry('ósmy', 8)), row(int_entry('dziewiąty', 9)));

        $ds = df()->read(from_rows($rows))->renameEach(rename_style(StringStyles::UCWORDS))->getEachAsArray();

        static::assertEquals(
            [
                ['Ósmy' => 8],
                ['Dziewiąty' => 9],
            ],
            \iterator_to_array($ds),
        );
    }

    public function test_rename_each_with_empty_map(): void
    {
        $rows = df()
            ->read(from_rows(rows(row(int_entry('id', 1), str_entry('name', 'foo')))))
            ->renameEach(rename_map([]))
            ->fetch();

        static::assertEquals(rows(row(int_entry('id', 1), str_entry('name', 'foo'))), $rows);
    }

    public function test_rename_each_with_map_chained_with_other_operations(): void
    {
        $ds = df()
            ->read(from_rows(rows(
                row(int_entry('user_id', 1), str_entry('user_name', 'John'), bool_entry('is_active', true)),
                row(int_entry('user_id', 2), str_entry('user_name', 'Jane'), bool_entry('is_active', false)),
            )))
            ->renameEach(rename_map(['user_id' => 'id', 'user_name' => 'name']))
            ->filter(ref('is_active')->equals(lit(true)))
            ->drop('is_active')
            ->getEachAsArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'John'],
            ],
            \iterator_to_array($ds),
        );
    }

    public function test_rename_each_with_map_multiple_entries(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                row(int_entry('id', 1), str_entry('first_name', 'John'), str_entry('last_name', 'Doe')),
                row(int_entry('id', 2), str_entry('first_name', 'Jane'), str_entry('last_name', 'Smith')),
            )))
            ->renameEach(rename_map([
                'first_name' => 'name',
                'last_name' => 'surname',
            ]))
            ->fetch();

        static::assertEquals(
            rows(
                row(int_entry('id', 1), str_entry('name', 'John'), str_entry('surname', 'Doe')),
                row(int_entry('id', 2), str_entry('name', 'Jane'), str_entry('surname', 'Smith')),
            ),
            $rows,
        );
    }

    public function test_rename_each_with_map_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata']);

        $rows = df()
            ->read(from_rows(rows(row(str_entry('old_name', 'value', $metadata)))))
            ->renameEach(rename_map(['old_name' => 'new_name']))
            ->fetch();

        static::assertTrue($rows->first()->get('new_name')->definition()->metadata()->isEqual($metadata));
    }

    public function test_rename_each_with_multiple_strategies(): void
    {
        $rows = rows(row(int_entry('ÓSMY', 8)), row(int_entry('DZIEWIĄTY', 9)), row(int_entry('ÓSMY I DZIEWIĄTY', 89)));

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
                ['osmy' => 8],
                ['dziewiaty' => 9],
                ['osmy-i-dziewiaty' => 89],
            ],
            \iterator_to_array($ds),
        );
    }

    public function test_rename_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata']);

        $rows = df()
            ->read(from_rows(rows(row(str_entry('old_name', 'value', $metadata)))))
            ->rename('old_name', 'new_name')
            ->fetch();

        static::assertTrue($rows->first()->get('new_name')->definition()->metadata()->isEqual($metadata));
    }
}
