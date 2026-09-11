<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Mother;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;

final class WideTableMother
{
    /**
     * @return list<array<string, int>>
     */
    public static function changedRows(int $columns, int $rows): array
    {
        $data = [];

        for ($id = 1; $id <= $rows; $id++) {
            $row = ['c1' => $id];

            for ($column = 2; $column <= $columns; $column++) {
                $row['c' . $column] = -$id;
            }

            $data[] = $row;
        }

        return $data;
    }

    /**
     * @return list<array<string, int>>
     */
    public static function rows(int $columns, int $rows): array
    {
        $data = [];

        for ($id = 1; $id <= $rows; $id++) {
            $row = [];

            for ($column = 1; $column <= $columns; $column++) {
                $row['c' . $column] = $id;
            }

            $data[] = $row;
        }

        return $data;
    }

    public static function table(string $name, int $columns): Table
    {
        $definitions = [];

        for ($column = 1; $column <= $columns; $column++) {
            $definitions[] = new Column('c' . $column, Type::getType(Types::INTEGER), ['notnull' => true]);
        }

        return new Table($name, $definitions);
    }

    public static function tableWithPrimaryKey(string $name, int $columns): Table
    {
        return self::table($name, $columns)
            ->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('c1')->create());
    }
}
