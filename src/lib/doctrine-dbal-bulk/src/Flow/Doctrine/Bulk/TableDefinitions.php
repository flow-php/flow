<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Connection;

final class TableDefinitions
{
    /**
     * @var array<string, TableDefinition>
     */
    private array $tables = [];

    public function __construct()
    {
    }

    public function get(string $name, Connection $connection) : TableDefinition
    {
        foreach ($this->tables as $table) {
            if ($table->name() === $name) {
                return $table;
            }
        }

        $this->tables[$name] = new TableDefinition($name, $connection);

        return $this->tables[$name];
    }
}
