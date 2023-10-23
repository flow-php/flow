<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\Column;
use Flow\Doctrine\Bulk\Exception\RuntimeException;

final class TableDefinition
{
    /**
     * @var Column[]
     */
    private array $columns;

    private string $name;

    public function __construct(string $name, Column ...$columns)
    {
        $this->name = $name;
        $this->columns = $columns;
    }

    /**
     * @throws RuntimeException
     */
    public function dbalColumn(string $columnName) : Column
    {
        $dbColumnNames = \array_filter($this->columns, fn (Column $dbColumn) : bool => $dbColumn->getName() === $columnName);

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
     * @return array<string, string>
     *
     * @psalm-suppress DeprecatedMethod
     */
    public function dbalTypes(BulkData $bulkData) : array
    {
        $types = [];

        foreach ($bulkData->columns()->all() as $columnName) {
            $dbColumn = $this->dbalColumn($columnName);

            for ($i = 0; $i < $bulkData->count(); $i++) {
                $types[$columnName . '_' . $i] = $dbColumn->getType()->getName();
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
                        $dbColumn = $this->dbalColumn($columnName);
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
}
