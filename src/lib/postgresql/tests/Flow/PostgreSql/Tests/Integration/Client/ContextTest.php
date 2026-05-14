<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use Flow\PostgreSql\Client\RowMapper;
use Flow\PostgreSql\Client\RowMapper\Context;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function Flow\PostgreSql\DSL\pgsql_client;
use function Flow\PostgreSql\DSL\pgsql_connection_dsn;
use function Flow\PostgreSql\DSL\postgresql_context;
use function Flow\Types\DSL\type_integer;

/**
 * @implements RowMapper<array{row_value: mixed, tenant_id: int, side_query_value: int}>
 */
final class ContextTestRowMapper implements RowMapper
{
    /**
     * @return array{row_value: mixed, tenant_id: int, side_query_value: int}
     */
    public function map(array $row, Context $context): array
    {
        $tenant = $context->get('tenant_id', type_integer());
        $sideQueryValue = $context->client()->fetchScalarInt('SELECT 99');

        return [
            'row_value' => $row['value'],
            'tenant_id' => $tenant,
            'side_query_value' => $sideQueryValue,
        ];
    }
}

final class ContextTest extends PostgreSqlTestCase
{
    public function test_mapper_reads_user_data_and_executes_side_query_via_client(): void
    {
        $dsn = \getenv('PGSQL_DATABASE_URL');
        static::assertNotFalse($dsn);

        $client = pgsql_client(pgsql_connection_dsn($dsn), null, postgresql_context(['tenant_id' => 42]));

        try {
            $result = $client->fetchInto(new ContextTestRowMapper(), 'SELECT 7 AS value');

            static::assertSame(
                [
                    'row_value' => 7,
                    'tenant_id' => 42,
                    'side_query_value' => 99,
                ],
                $result,
            );
        } finally {
            $client->close();
        }
    }
}
