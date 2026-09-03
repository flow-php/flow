<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Context;

use mysqli;
use mysqli_result;
use RuntimeException;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_numeric_string;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final readonly class MysqlSessionStatus
{
    public function __construct(
        private mysqli $connection,
    ) {}

    /**
     * Read through mysqli::query(), a plain text query, so the read itself moves Com_query and
     * never the Com_stmt_* counters this context exists to observe.
     */
    public function of(string $name): int
    {
        $result = $this->connection->query("SHOW SESSION STATUS LIKE '{$name}'");

        if (!$result instanceof mysqli_result) {
            throw new RuntimeException("SHOW SESSION STATUS LIKE '{$name}' returned no result");
        }

        $row = type_structure(['Variable_name' => type_string(), 'Value' => type_numeric_string()])->assert(
            $result->fetch_assoc(),
        );

        return type_integer()->cast($row['Value']);
    }
}
