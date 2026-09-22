<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Cardinality;
use Flow\PostgreSql\AST\Transformers\ExplainConfig;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\QueryBuilder\Sql;

use function min;

final readonly class ExplainedRows
{
    /**
     * The planner's row estimate for $query, capped by the read's maximum. EXPLAIN without ANALYZE, so the query
     * never runs.
     *
     * @param list<mixed> $parameters
     */
    public function of(Client $client, string|Sql $query, array $parameters = [], ?int $maximum = null): Cardinality
    {
        $estimate = $client->explain($query, $parameters, ExplainConfig::forEstimate())->rootNode()->estimatedRows();

        return new Cardinality(
            atMost: $maximum,
            estimate: $maximum === null ? $estimate : min($estimate, $maximum),
            relativeError: Cardinality::DEFAULT_RELATIVE_ERROR,
        );
    }
}
