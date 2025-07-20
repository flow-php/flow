<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Types\Type;
use Flow\Doctrine\Bulk\Exception\RuntimeException;

final class TableDefinition
{
    /**
     * @var null|array<Column>
     */
    private ?array $columns = null;

    public function __construct(private readonly string $name, private readonly Connection $connection)
    {
    }

    /**
     * @throws RuntimeException
     */
    public function dbalColumn(string $columnName) : Column
    {

        $dbColumnNames = \array_filter($this->getColumns(), fn (Column $dbColumn) : bool => $dbColumn->getName() === $columnName);

        if (\count($dbColumnNames) !== 1) {
            throw new RuntimeException("Column with name {$columnName}, not found in table: {$this->name}");
        }

        return \current($dbColumnNames);
    }

    /**
     * @param BulkData $bulkData
     *
     * @throws RuntimeException
     *
     * @return array<int<0, max>|string, string>
     */
    public function dbalParameterTypes(BulkData $bulkData) : array
    {
        return match ($bulkData->parametersStyle()) {
            SQLParametersStyle::NAMED => $this->dbalTypes($bulkData),
            SQLParametersStyle::POSITIONAL => $this->dbalPositionalTypes($bulkData),
        };
    }

    /**
     * @return array<int<0, max>, string>
     */
    public function dbalPositionalTypes(BulkData $bulkData) : array
    {
        $types = [];

        for ($i = 0; $i < $bulkData->count(); $i++) {
            foreach ($bulkData->columns()->all() as $columnName) {
                $dbColumn = $this->dbalColumn($columnName);
                $types[] = Type::getTypeRegistry()->lookupName($dbColumn->getType());
            }
        }

        return $types;
    }

    /**
     * @param BulkData $bulkData
     *
     * @throws RuntimeException
     *
     * @return array<string, string>
     */
    public function dbalTypes(BulkData $bulkData) : array
    {
        $types = [];

        foreach ($bulkData->columns()->all() as $columnName) {
            $dbColumn = $this->dbalColumn($columnName);

            for ($i = 0; $i < $bulkData->count(); $i++) {
                $types[$columnName . '_' . $i] = Type::getTypeRegistry()->lookupName($dbColumn->getType());
            }
        }

        return $types;
    }

    /**
     * @return string
     */
    public function name() : string
    {
        return $this->name;
    }

    public function platform() : AbstractPlatform
    {
        return $this->connection->getDatabasePlatform();
    }

    /**
     * @return array<Column>
     */
    private function getColumns() : array
    {
        if ($this->columns !== null) {
            return $this->columns;
        }

        $this->columns = array_values($this->connection->createSchemaManager()->listTableColumns($this->name));

        return $this->columns;
    }
}
