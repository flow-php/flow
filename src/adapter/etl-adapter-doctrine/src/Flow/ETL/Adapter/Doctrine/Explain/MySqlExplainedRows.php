<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Explain;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Types\Type;

use function explode;
use function is_numeric;
use function is_string;
use function preg_match;
use function round;

final readonly class MySqlExplainedRows
{
    /**
     * The root line of EXPLAIN FORMAT=TREE, e.g. "-> Filter: (t.g > 1)  (cost=0.75 rows=2)". A root without an
     * estimate (a GROUP BY read back from "<temporary>") is unknown: a child's estimate counts other rows.
     *
     * @param array<string, mixed>|list<mixed> $parameters
     * @param array<int<0, max>|string, ArrayParameterType|ParameterType|string|Type> $types
     */
    public function of(Connection $connection, string $sql, array $parameters = [], array $types = []): ?int
    {
        $plan = $connection->fetchOne('EXPLAIN FORMAT=TREE ' . $sql, $parameters, $types);

        if (!is_string($plan)) {
            return null;
        }

        $matches = [];

        if (preg_match('/\brows=(\d+(?:\.\d+)?(?:e[+-]?\d+)?)/i', explode("\n", $plan)[0], $matches) !== 1) {
            return null;
        }

        if (!is_numeric($matches[1])) {
            return null;
        }

        return (int) round((float) $matches[1]);
    }
}
