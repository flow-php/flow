<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Integration\IntegrationTestCase;
use Flow\ETL\Transformer\StyleConverter\StringStyles;

final class RenameTest extends IntegrationTestCase
{
    public function test_rename() : void
    {
        $rows = df()
            ->read(from_rows(
                new Rows(
                    Row::create(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                    Row::create(int_entry('id', 2), null_entry('name'), bool_entry('active', false)),
                    Row::create(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
                )
            ))
            ->rename('name', 'new_name')
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('new_name', 'foo'), bool_entry('active', true)),
                Row::create(int_entry('id', 2), null_entry('new_name'), bool_entry('active', false)),
                Row::create(int_entry('id', 2), str_entry('new_name', 'bar'), bool_entry('active', false)),
            ),
            $rows
        );
    }

    public function test_rename_all() : void
    {
        $rows = new Rows(
            Row::create(array_entry('array', ['id' => 1, 'name' => 'name', 'active' => true])),
            Row::create(array_entry('array', ['id' => 2, 'name' => 'name', 'active' => false]))
        );

        $ds = df()
            ->read(from_rows($rows))
            ->withEntry('row', ref('array')->unpack())
            ->renameAll('row.', '')
            ->drop('array')
            ->getEachAsArray();

        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'name', 'active' => true],
                ['id' => 2, 'name' => 'name', 'active' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_lower_case() : void
    {
        $rows = new Rows(
            Row::create(int_entry('ID', 1), str_entry('NAME', 'name'), bool_entry('ACTIVE', true)),
            Row::create(int_entry('ID', 2), str_entry('NAME', 'name'), bool_entry('ACTIVE', false)),
        );

        $ds = df()->read(from_rows($rows))->renameAllLowerCase()->getEachAsArray();

        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'name', 'active' => true],
                ['id' => 2, 'name' => 'name', 'active' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_to_snake_case() : void
    {
        $rows = new Rows(
            Row::create(int_entry('id', 1), str_entry('UserName', 'name'), bool_entry('isActive', true)),
            Row::create(int_entry('id', 2), str_entry('UserName', 'name'), bool_entry('isActive', false)),
        );

        $ds = df()
            ->read(from_rows($rows))
            ->renameAllStyle(StringStyles::SNAKE)
            ->renameAllLowerCase()
            ->getEachAsArray();

        $this->assertEquals(
            [
                ['id' => 1, 'user_name' => 'name', 'is_active' => true],
                ['id' => 2, 'user_name' => 'name', 'is_active' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_upper_case() : void
    {
        $rows = new Rows(
            Row::create(int_entry('id', 1), str_entry('name', 'name'), bool_entry('active', true)),
            Row::create(int_entry('id', 2), str_entry('name', 'name'), bool_entry('active', false)),
        );

        $ds = df()
            ->read(from_rows($rows))
            ->renameAllUpperCase()
            ->getEachAsArray();

        $this->assertEquals(
            [
                ['ID' => 1, 'NAME' => 'name', 'ACTIVE' => true],
                ['ID' => 2, 'NAME' => 'name', 'ACTIVE' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_upper_case_first() : void
    {
        $rows = new Rows(
            Row::create(int_entry('id', 1), str_entry('name', 'name'), bool_entry('active', true)),
            Row::create(int_entry('id', 2), str_entry('name', 'name'), bool_entry('active', false)),
        );

        $ds = df()
            ->read(from_rows($rows))
            ->renameAllUpperCaseFirst()
            ->getEachAsArray();

        $this->assertEquals(
            [
                ['Id' => 1, 'Name' => 'name', 'Active' => true],
                ['Id' => 2, 'Name' => 'name', 'Active' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_upper_case_word() : void
    {
        $rows = new Rows(
            Row::create(int_entry('id', 1), str_entry('name', 'name'), bool_entry('active', true)),
            Row::create(int_entry('id', 2), str_entry('name', 'name'), bool_entry('active', false)),
        );

        $ds = df()
            ->read(from_rows($rows))
            ->renameAllUpperCaseWord()
            ->getEachAsArray();

        $this->assertEquals(
            [
                ['Id' => 1, 'Name' => 'name', 'Active' => true],
                ['Id' => 2, 'Name' => 'name', 'Active' => false],
            ],
            \iterator_to_array($ds)
        );
    }
}
