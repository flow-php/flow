<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Explain;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Types\Type;

use function array_key_exists;
use function is_array;
use function is_float;
use function is_int;
use function is_string;
use function json_decode;
use function round;

final readonly class PostgreSqlExplainedRows
{
    /**
     * The root plan node's "Plan Rows"; EXPLAIN without ANALYZE, so the query never runs.
     *
     * @param array<string, mixed>|list<mixed> $parameters
     * @param array<int<0, max>|string, ArrayParameterType|ParameterType|string|Type> $types
     */
    public function of(Connection $connection, string $sql, array $parameters = [], array $types = []): ?int
    {
        $plan = $connection->fetchOne('EXPLAIN (FORMAT JSON) ' . $sql, $parameters, $types);

        // @mago-expect analysis:mixed-assignment
        $plan = is_string($plan) ? json_decode($plan, true) : $plan;

        if (!is_array($plan) || !array_key_exists(0, $plan) || !is_array($plan[0])) {
            return null;
        }

        // @mago-expect analysis:mixed-assignment
        $root = $plan[0]['Plan'] ?? null;

        if (!is_array($root) || !array_key_exists('Plan Rows', $root)) {
            return null;
        }

        // @mago-expect analysis:mixed-assignment
        $rows = $root['Plan Rows'];

        return is_int($rows) || is_float($rows) ? (int) round($rows) : null;
    }
}
