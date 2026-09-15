<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use PgSql\Connection as PgSqlConnection;

use function array_fill;
use function pg_field_name;
use function pg_field_type;
use function pg_free_result;
use function pg_get_result;
use function pg_last_error;
use function pg_num_fields;
use function pg_result_error_field;
use function pg_result_status;
use function pg_send_query_params;
use function sprintf;
use function trim;

use const PGSQL_DIAG_MESSAGE_PRIMARY;
use const PGSQL_DIAG_SQLSTATE;
use const PGSQL_TUPLES_OK;

final readonly class PgSqlResultColumns
{
    /**
     * @throws ProbeRefusal
     *
     * @return list<ResultColumn>
     */
    public function of(PgSqlConnection $connection, string $probe): array
    {
        // DBAL emits :name, libpq speaks $1..$n.
        $rewritten = (new NativePlaceholders())->toPostgreSql($probe);

        // pg_query_params() returns false and keeps no SQLSTATE; the result of a sent query carries it
        if (!@pg_send_query_params($connection, $rewritten->sql, array_fill(0, $rewritten->parameters, null))) {
            throw new ProbeRefusal(sprintf(
                'PostgreSQL refused the zero-row probe of this query (%s)',
                trim(pg_last_error($connection)),
            ));
        }

        $result = pg_get_result($connection);

        // drain every result, so DBAL finds the connection idle
        while (pg_get_result($connection) !== false) {
        }

        if ($result === false) {
            throw new ProbeRefusal(sprintf(
                'PostgreSQL refused the zero-row probe of this query (%s)',
                trim(pg_last_error($connection)),
            ));
        }

        try {
            if (pg_result_status($result) !== PGSQL_TUPLES_OK) {
                throw new ProbeRefusal(
                    sprintf(
                        'PostgreSQL refused the zero-row probe of this query (%s)',
                        (string) pg_result_error_field($result, PGSQL_DIAG_MESSAGE_PRIMARY),
                    ),
                    0,
                    pg_result_error_field($result, PGSQL_DIAG_SQLSTATE) ?: null,
                );
            }

            $columns = [];

            for ($i = 0; $i < pg_num_fields($result); $i++) {
                $columns[] = new ResultColumn(pg_field_name($result, $i), pg_field_type($result, $i));
            }

            return $columns;
        } finally {
            pg_free_result($result);
        }
    }
}
