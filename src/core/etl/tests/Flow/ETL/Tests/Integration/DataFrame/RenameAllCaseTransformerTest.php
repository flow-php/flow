<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{bool_entry, df, from_rows, int_entry, str_entry};
use function Flow\ETL\DSL\{row, rows};
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\{Transformer\RenameAllCaseTransformer};
use PHPUnit\Framework\Attributes\{IgnoreDeprecations};

#[IgnoreDeprecations]
final class RenameAllCaseTransformerTest extends FlowIntegrationTestCase
{
    public function test_rename_all_lower_case() : void
    {
        $rows = rows(row(int_entry('ID', 1), str_entry('NAME', 'name'), bool_entry('ACTIVE', true)), row(int_entry('ID', 2), str_entry('NAME', 'name'), bool_entry('ACTIVE', false)));

        $ds = df()
            ->read(from_rows($rows))
            ->transform(new RenameAllCaseTransformer(lower: true))
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
            ->transform(new RenameAllCaseTransformer(lower: true))
            ->getEachAsArray();

        self::assertEquals(
            [
                ['ilość przedmiotów' => 0],
                ['ilość przedmiotów' => 10],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_upper_case() : void
    {
        $rows = rows(row(int_entry('id', 1), str_entry('name', 'name'), bool_entry('active', true)), row(int_entry('id', 2), str_entry('name', 'name'), bool_entry('active', false)));

        $ds = df()
            ->read(from_rows($rows))
            ->transform(new RenameAllCaseTransformer(upper: true))
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
            ->transform(new RenameAllCaseTransformer(ucfirst: true))
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
            ->transform(new RenameAllCaseTransformer(ucwords: true))
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
            ->transform(new RenameAllCaseTransformer(ucwords: true))
            ->getEachAsArray();

        self::assertEquals(
            [
                ['Ósmy' => 8],
                ['Dziewiąty' => 9],
            ],
            \iterator_to_array($ds)
        );
    }
}
