<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\read;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Integration\IntegrationTestCase;
use Flow\ETL\Transformer\StyleConverter\StringStyles;

final class RenameTest extends IntegrationTestCase
{
    public function test_rename() : void
    {
        $rows = read(from_rows(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'foo'), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 2), Entry::null('name'), Entry::boolean('active', false)),
                Row::create(Entry::integer('id', 2), Entry::string('name', 'bar'), Entry::boolean('active', false)),
            )
        ))
            ->rename('name', 'new_name')
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('new_name', 'foo'), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 2), Entry::null('new_name'), Entry::boolean('active', false)),
                Row::create(Entry::integer('id', 2), Entry::string('new_name', 'bar'), Entry::boolean('active', false)),
            ),
            $rows
        );
    }

    public function test_rename_all() : void
    {
        $rows = new Rows(
            Row::create(Entry::array('array', ['id' => 1, 'name' => 'name', 'active' => true])),
            Row::create(Entry::array('array', ['id' => 2, 'name' => 'name', 'active' => false]))
        );

        $ds = read(from_rows($rows))
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
            Row::create(Entry::int('ID', 1), Entry::str('NAME', 'name'), Entry::bool('ACTIVE', true)),
            Row::create(Entry::int('ID', 2), Entry::str('NAME', 'name'), Entry::bool('ACTIVE', false)),
        );

        $ds = read(from_rows($rows))->renameAllLowerCase()->getEachAsArray();

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
            Row::create(Entry::int('id', 1), Entry::str('UserName', 'name'), Entry::bool('isActive', true)),
            Row::create(Entry::int('id', 2), Entry::str('UserName', 'name'), Entry::bool('isActive', false)),
        );

        $ds = read(from_rows($rows))
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
            Row::create(Entry::int('id', 1), Entry::str('name', 'name'), Entry::bool('active', true)),
            Row::create(Entry::int('id', 2), Entry::str('name', 'name'), Entry::bool('active', false)),
        );

        $ds = read(from_rows($rows))
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
            Row::create(Entry::int('id', 1), Entry::str('name', 'name'), Entry::bool('active', true)),
            Row::create(Entry::int('id', 2), Entry::str('name', 'name'), Entry::bool('active', false)),
        );

        $ds = read(from_rows($rows))
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
            Row::create(Entry::int('id', 1), Entry::str('name', 'name'), Entry::bool('active', true)),
            Row::create(Entry::int('id', 2), Entry::str('name', 'name'), Entry::bool('active', false)),
        );

        $ds = read(from_rows($rows))
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
