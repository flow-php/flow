<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Flow\Doctrine\Bulk\Exception\RuntimeException;

final class BulkData
{
    private Columns $columns;

    /**
     * @var array<int, array<string, mixed>>
     */
    private array $rows;

    /**
     * @psalm-suppress DocblockTypeContradiction
     *
     * @param array<int, array<string, mixed>> $rows
     */
    public function __construct(array $rows)
    {
        if (empty($rows)) {
            throw new RuntimeException('Bulk data cannot be empty');
        }

        $firstRow = \reset($rows);

        if (!\is_array($firstRow)) {
            throw new RuntimeException('Each row must be an array');
        }

        $keys = \array_keys($firstRow);

        foreach ($rows as $row) {
            if (!\is_array($row)) {
                throw new RuntimeException('Each row must be an array');
            }

            if ($keys !== \array_keys($row)) {
                throw new RuntimeException('Each row must be have the same keys in the same order');
            }
        }

        $this->columns = new Columns(...$keys);
        $this->rows = \array_map( /** @phpstan-ignore-line */
            fn (int $index, array $row) => \array_combine(
                $this->columns->suffix("_{$index}")->all(),
                $row,
            ),
            \range(0, \count($rows) - 1),
            $rows
        );
    }

    /**
     * @return array<string, mixed>
     */
    public function toSqlParameters() : array
    {
        return \array_merge(...$this->rows);
    }

    /**
     * @return array<string>
     */
    public function toTypes() : array
    {
        return  \array_map(
            fn ($value) : string => \gettype($value),
            \array_filter($this->toSqlParameters(), fn ($value) : bool => \is_bool($value))
        );
    }

    /**
     * @return string It returns a string for SQL bulk insert query, eg:
     *                (:id_0, :name_0, :title_0), (:id_1, :name_1, :title_1), (:id_2, :name_2, :title_2)
     */
    public function toSqlValuesPlaceholders() : string
    {
        return \implode(
            ',',
            \array_map(
                fn (array $row) : string => \sprintf(
                    '(%s)',
                    (new Columns(...\array_keys($row)))->prefix(':')->concat(',')
                ),
                $this->rows
            )
        );
    }

    public function columns() : Columns
    {
        return $this->columns;
    }
}
