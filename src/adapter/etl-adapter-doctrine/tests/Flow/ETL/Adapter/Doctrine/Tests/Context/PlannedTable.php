<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Context;

use Doctrine\DBAL\Platforms\AbstractMySQLPlatform;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;

final class PlannedTable
{
    /**
     * An (id, grp) table of $rows rows with fresh planner statistics, so EXPLAIN estimates what it holds.
     */
    public static function create(DatabaseContext $context, string $name, int $rows): void
    {
        $context->createTable(
            (new Table($name, [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('grp', Type::getType(Types::INTEGER), ['notnull' => true]),
            ]))->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('id')->create()),
        );

        for ($id = 1; $id <= $rows; $id++) {
            $context->insert($name, ['id' => $id, 'grp' => $id % 3]);
        }

        // DBAL has no statement for refreshing planner statistics
        $context
            ->connection()
            ->executeStatement(
                $context->connection()->getDatabasePlatform() instanceof AbstractMySQLPlatform
                    ? 'ANALYZE TABLE ' . $name
                    : 'ANALYZE ' . $name,
            );
    }
}
