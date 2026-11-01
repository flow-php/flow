<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Connection;
use WeakMap;

use function array_key_exists;

final class TableDefinitions
{
    /**
     * @var WeakMap<Connection, array<string, TableDefinition>>
     */
    private WeakMap $tables;

    public function __construct()
    {
        $this->tables = new WeakMap();
    }

    public function get(string $name, Connection $connection): TableDefinition
    {
        $tables = $this->tables[$connection] ?? [];

        if (!array_key_exists($name, $tables)) {
            $tables[$name] = new TableDefinition($name, $connection);
            $this->tables[$connection] = $tables;
        }

        return $tables[$name];
    }
}
