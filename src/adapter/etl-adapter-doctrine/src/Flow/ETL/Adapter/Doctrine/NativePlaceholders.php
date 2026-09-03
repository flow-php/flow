<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

/**
 * Rewrites DBAL's :name / ? placeholders into a driver-native dialect, using DBAL's own SQL parser.
 */
final readonly class NativePlaceholders
{
    public function toMysqli(string $sql): RewrittenSql
    {
        return (new PlaceholderRewriter(new MysqliPlaceholders()))->rewrite($sql);
    }

    public function toPostgreSql(string $sql): RewrittenSql
    {
        return (new PlaceholderRewriter(new PostgreSqlPlaceholders()))->rewrite($sql);
    }
}
