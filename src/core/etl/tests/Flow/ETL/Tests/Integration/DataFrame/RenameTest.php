<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{bool_entry, df, from_rows, int_entry, json_entry, ref, str_entry};
use function Flow\ETL\DSL\{rename_replace, rename_style, row, rows};
use Flow\ETL\String\StringStyles;
use Flow\ETL\Tests\FlowIntegrationTestCase;

final class RenameTest extends FlowIntegrationTestCase
{
    public function test_rename() : void
    {
        $rows = df()
            ->read(from_rows(
                rows(row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)), row(int_entry('id', 2), str_entry('name', null), bool_entry('active', false)), row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)))
            ))
            ->rename('name', 'new_name')
            ->fetch();

        self::assertEquals(
            rows(row(int_entry('id', 1), str_entry('new_name', 'foo'), bool_entry('active', true)), row(int_entry('id', 2), str_entry('new_name', null), bool_entry('active', false)), row(int_entry('id', 2), str_entry('new_name', 'bar'), bool_entry('active', false))),
            $rows
        );
    }

    public function test_rename_all() : void
    {
        $rows = rows(row(json_entry('array', ['id' => 1, 'name' => 'name', 'active' => true])), row(json_entry('array', ['id' => 2, 'name' => 'name', 'active' => false])));

        $ds = df()
            ->read(from_rows($rows))
            ->withEntry('row', ref('array')->unpack())
            ->renameEach(rename_replace('row.', ''))
            ->drop('array')
            ->getEachAsArray();

        self::assertEquals(
            [
                ['id' => 1, 'name' => 'name', 'active' => true],
                ['id' => 2, 'name' => 'name', 'active' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_lower_case() : void
    {
        $rows = rows(row(int_entry('ID', 1), str_entry('NAME', 'name'), bool_entry('ACTIVE', true)), row(int_entry('ID', 2), str_entry('NAME', 'name'), bool_entry('ACTIVE', false)));

        $ds = df()
            ->read(from_rows($rows))
            ->renameEach(rename_style(StringStyles::LOWER))
            ->getEachAsArray();

        self::assertEquals(
            [
                ['id' => 1, 'name' => 'name', 'active' => true],
                ['id' => 2, 'name' => 'name', 'active' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_lower_case_i18n() : void
    {
        $rows = rows(row(int_entry('ILOŚĆ PRZEDMIOTÓW', 0)), row(int_entry('ILOŚĆ PRZEDMIOTÓW', 10)));

        $ds = df()
            ->read(from_rows($rows))
            ->renameEach(rename_style(StringStyles::LOWER))
            ->getEachAsArray();

        self::assertEquals(
            [
                ['ilość przedmiotów' => 0],
                ['ilość przedmiotów' => 10],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_multiple() : void
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

        self::assertEquals(
            [
                ['id' => 1, 'name' => 'name', 'active' => true],
                ['id' => 2, 'name' => 'name', 'active' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_to_ascii() : void
    {
        $rows = rows(row(int_entry('ÓSMY', 8)), row(int_entry('DZIEWIĄTY', 9)));

        $ds = df()
            ->read(from_rows($rows))
            ->renameEach(rename_style(StringStyles::ASCII))
            ->getEachAsArray();

        self::assertEquals(
            [
                ['OSMY' => 8],
                ['DZIEWIATY' => 9],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_to_camel() : void
    {
        $rows = rows(row(int_entry('ósmy i dziewiąty', 89)));

        $ds = df()
            ->read(from_rows($rows))
            ->renameEach(rename_style(StringStyles::CAMEL))
            ->getEachAsArray();

        self::assertEquals(
            [
                ['ósmyIDziewiąty' => 89],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_to_slug() : void
    {
        $rows = rows(row(int_entry('ÓSMY I DZIEWIĄTY', 89)));

        $ds = df()
            ->read(from_rows($rows))
            ->renameEach(rename_style(StringStyles::SLUG))
            ->renameEach(rename_style(StringStyles::LOWER))
            ->getEachAsArray();

        self::assertEquals(
            [
                ['osmy-i-dziewiaty' => 89],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_to_snake_case() : void
    {
        $rows = rows(row(int_entry('id', 1), str_entry('UserName', 'name'), bool_entry('isActive', true)), row(int_entry('id', 2), str_entry('UserName', 'name'), bool_entry('isActive', false)));

        $ds = df()
            ->read(from_rows($rows))
            ->renameAllStyle(StringStyles::SNAKE)
            ->renameEach(rename_style(StringStyles::LOWER))
            ->getEachAsArray();

        self::assertEquals(
            [
                ['id' => 1, 'user_name' => 'name', 'is_active' => true],
                ['id' => 2, 'user_name' => 'name', 'is_active' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_to_title() : void
    {
        $rows = rows(row(int_entry('ósmy i dziewiąty', 89)));

        $ds = df()
            ->read(from_rows($rows))
            ->renameEach(rename_style(StringStyles::TITLE))
            ->getEachAsArray();

        self::assertEquals(
            [
                ['Ósmy i dziewiąty' => 89],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_upper_case() : void
    {
        $rows = rows(row(int_entry('id', 1), str_entry('name', 'name'), bool_entry('active', true)), row(int_entry('id', 2), str_entry('name', 'name'), bool_entry('active', false)));

        $ds = df()
            ->read(from_rows($rows))
            ->renameEach(rename_style(StringStyles::UPPER))
            ->getEachAsArray();

        self::assertEquals(
            [
                ['ID' => 1, 'NAME' => 'name', 'ACTIVE' => true],
                ['ID' => 2, 'NAME' => 'name', 'ACTIVE' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_upper_case_first() : void
    {
        $rows = rows(row(int_entry('id', 1), str_entry('name', 'name'), bool_entry('active', true)), row(int_entry('id', 2), str_entry('name', 'name'), bool_entry('active', false)));

        $ds = df()
            ->read(from_rows($rows))
            ->renameEach(rename_style(StringStyles::UCFIRST))
            ->getEachAsArray();

        self::assertEquals(
            [
                ['Id' => 1, 'Name' => 'name', 'Active' => true],
                ['Id' => 2, 'Name' => 'name', 'Active' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_upper_case_word() : void
    {
        $rows = rows(row(int_entry('id', 1), str_entry('name', 'name'), bool_entry('active', true)), row(int_entry('id', 2), str_entry('name', 'name'), bool_entry('active', false)));

        $ds = df()
            ->read(from_rows($rows))
            ->renameEach(rename_style(StringStyles::UCWORDS))
            ->getEachAsArray();

        self::assertEquals(
            [
                ['Id' => 1, 'Name' => 'name', 'Active' => true],
                ['Id' => 2, 'Name' => 'name', 'Active' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_upper_case_word_i18n() : void
    {
        $rows = rows(row(int_entry('ósmy', 8)), row(int_entry('dziewiąty', 9)));

        $ds = df()
            ->read(from_rows($rows))
            ->renameEach(rename_style(StringStyles::UCWORDS))
            ->getEachAsArray();

        self::assertEquals(
            [
                ['Ósmy' => 8],
                ['Dziewiąty' => 9],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_each_with_multiple_strategies() : void
    {
        $rows = rows(
            row(int_entry('ÓSMY', 8)),
            row(int_entry('DZIEWIĄTY', 9)),
            row(int_entry('ÓSMY I DZIEWIĄTY', 89)),
        );

        $ds = df()
            ->read(from_rows($rows))
            ->renameEach(
                rename_style(StringStyles::ASCII),
                rename_style(StringStyles::LOWER),
                rename_style(StringStyles::KEBAB),
            )
            ->getEachAsArray();

        self::assertEquals(
            [
                ['osmy' => 8],
                ['dziewiaty' => 9],
                ['osmy-i-dziewiaty' => 89],
            ],
            \iterator_to_array($ds)
        );
    }
}
