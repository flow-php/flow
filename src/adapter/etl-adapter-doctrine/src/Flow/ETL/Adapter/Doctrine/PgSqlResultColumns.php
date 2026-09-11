<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Flow\ETL\Exception\SchemaNotDerivableException;
use PgSql\Connection as PgSqlConnection;

use function array_fill;
use function pg_field_name;
use function pg_field_type;
use function pg_free_result;
use function pg_last_error;
use function pg_num_fields;
use function pg_query_params;
use function sprintf;
use function trim;

final readonly class PgSqlResultColumns
{
    /**
     * @param class-string $extractor
     *
     * @throws SchemaNotDerivableException
     *
     * @return list<ResultColumn>
     */
    public function of(PgSqlConnection $connection, string $probe, string $extractor): array
    {
        // DBAL emits :name, libpq speaks $1..$n.
        $rewritten = (new NativePlaceholders())->toPostgreSql($probe);
        $result = @pg_query_params($connection, $rewritten->sql, array_fill(0, $rewritten->parameters, null));

        if ($result === false) {
            throw SchemaNotDerivableException::extractor($extractor, sprintf(
                'PostgreSQL refused the zero-row probe of this query (%s)',
                trim(pg_last_error($connection)),
            ));
        }

        try {
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
