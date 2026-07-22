<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service;

final class Services
{
    public static function pgsqlDsn(): string
    {
        return (string) getenv('PGSQL_DATABASE_URL');
    }

    public static function elasticsearchUrl(): string
    {
        return (string) getenv('ELASTICSEARCH_URL');
    }
}
