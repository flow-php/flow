<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\Column;
use Flow\Doctrine\Bulk\Exception\RuntimeException;

final class TableDefinition
{
    private string $name;

    /**
     * @var Column[]
     */
    private array $columns;

    public function __construct(string $name, Column ...$columns)
    {
        $this->name = $name;
        $this->columns = $columns;
    }

    /**
     * @return string
     */
    public function name() : string
    {
        return $this->name;
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
            $dbColumn = $this->getDbalColumn($columnName);

            for ($i = 0; $i < $bulkData->count(); $i++) {
                $types[$columnName . '_' . $i] = $dbColumn->getType()->getName();
            }
        }

        return $types;
    }

    /**
     * @psalm-suppress UnusedForeachValue
     */
    public function toSqlCastedPlaceholders(BulkData $bulkData, AbstractPlatform $abstractPlatform) : string
    {
        return \implode(
            ',',
            \array_map(
                /**
                 * @param int $index
                 * @param array<string, mixed> $row
                 *
                 * @return string
                 */
                function (int $index, array $row) use ($abstractPlatform) : string {
                    $keys = [];
                    /**
                     * @var mixed $value
                     */
                    foreach ($row as $columnName => $value) {
                        $dbColumn = $this->getDbalColumn($columnName);
                        $keys[] = 'CAST(:' . $columnName . '_' . $index . ' as ' . $dbColumn->getType()->getSQLDeclaration($dbColumn->toArray(), $abstractPlatform) . ')';
                    }

                    return \sprintf(
                        '(%s)',
                        \implode(',', $keys)
                    );
                },
                \array_keys($bulkData->rows()),
                $bulkData->rows(),
            )
        );
    }

    /**
     * @throws RuntimeException
     */
    private function getDbalColumn(string $columnName) : Column
    {
        $dbColumnNames = \array_filter($this->columns, fn (Column $dbColumn) : bool => $dbColumn->getName() === $columnName);

        if (\count($dbColumnNames) !== 1) {
            throw new RuntimeException("Column with name {$columnName}, not found in table: {$this->name}");
        }

        return \current($dbColumnNames);
    }
}
