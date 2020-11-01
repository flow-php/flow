<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\QueryFactory;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\Exception\RuntimeException;
use Flow\Doctrine\Bulk\QueryFactory;

class DbalQueryFactory implements QueryFactory
{
    public function insert(AbstractPlatform $platform, string $table, BulkData $bulkData) : string
    {
        if ($platform->getName() === 'postgresql') {
            return \sprintf(
                'INSERT INTO %s (%s) VALUES %s',
                $table,
                $bulkData->columns()->concat(','),
                $bulkData->toSqlValuesPlaceholders()
            );
        }

        throw new RuntimeException(\sprintf(
            'Database platform "%s" is not supported by this factory',
            $platform->getName()
        ));
    }

    public function insertOrSkipOnConflict(AbstractPlatform $platform, string $table, BulkData $bulkData) : string
    {
        if ($platform->getName() === 'postgresql') {
            return \sprintf(
                'INSERT INTO %s (%s) VALUES %s ON CONFLICT DO NOTHING',
                $table,
                $bulkData->columns()->concat(','),
                $bulkData->toSqlValuesPlaceholders()
            );
        }

        throw new RuntimeException(\sprintf(
            'Database platform "%s" is not supported by this factory',
            $platform->getName()
        ));
    }

    public function insertOrUpdateOnConstraintConflict(AbstractPlatform $platform, string $table, string $constraint, BulkData $bulkData) : string
    {
        if ($platform->getName() === 'postgresql') {
            return \sprintf(
                'INSERT INTO %s (%s) VALUES %s ON CONFLICT ON CONSTRAINT %s DO UPDATE SET %s',
                $table,
                $bulkData->columns()->concat(','),
                $bulkData->toSqlValuesPlaceholders(),
                $constraint,
                /**
                 * https://www.postgresql.org/docs/9.5/sql-insert.html#SQL-ON-CONFLICT
                 * The SET and WHERE clauses in ON CONFLICT DO UPDATE have access to the existing row using the
                 * table's name (or an alias), and to rows proposed for insertion using the special EXCLUDED table.
                 */
                \implode(
                    ',',
                    $bulkData->columns()->map(
                        fn (string $column) : string => "{$column} = excluded.{$column}"
                    )
                )
            );
        }

        throw new RuntimeException(\sprintf(
            'Database platform "%s" is not supported by this factory',
            $platform->getName()
        ));
    }
}
