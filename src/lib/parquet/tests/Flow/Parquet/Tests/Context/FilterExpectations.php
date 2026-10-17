<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use Flow\Parquet\Filter\Evaluation\FilterEvaluator;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\Reader\RowSelection;

use function array_values;
use function intdiv;

final class FilterExpectations
{
    /**
     * The row groups a selection reads rows of.
     *
     * @return list<int>
     */
    public static function keptGroups(RowSelection $selection, int $rowsPerGroup): array
    {
        $rowCounts = [];

        for ($covered = 0; $covered < $selection->totalRows(); $covered += $rowsPerGroup) {
            $rowCounts[] = $rowsPerGroup;
        }

        $kept = [];

        foreach ($selection->split($rowCounts) as $group => $rows) {
            if ($rows->selectsAny()) {
                $kept[] = $group;
            }
        }

        return $kept;
    }

    /**
     * The row groups holding a row, from `$offset` on, the filter keeps by FilterEvaluator.
     *
     * @param iterable<array<string, mixed>> $rows
     *
     * @return list<int>
     */
    public static function matchingGroups(
        iterable $rows,
        Filter $filter,
        Schema $schema,
        int $rowsPerGroup,
        int $offset = 0,
    ): array {
        $groups = [];

        foreach (self::matchingIds($rows, $filter, $schema, $offset) as $id) {
            $groups[intdiv($id, $rowsPerGroup)] = intdiv($id, $rowsPerGroup);
        }

        return array_values($groups);
    }

    /**
     * The ids of the rows, from `$offset` on, the filter keeps by FilterEvaluator.
     *
     * @param iterable<array<string, mixed>> $rows
     *
     * @return list<int>
     */
    public static function matchingIds(iterable $rows, Filter $filter, Schema $schema, int $offset = 0): array
    {
        $batch = [];
        $index = 0;

        foreach ($rows as $row) {
            if ($index++ < $offset) {
                continue;
            }

            // @mago-ignore analysis:mixed-assignment
            foreach ($row as $column => $value) {
                $batch[$column][] = $value;
            }
        }

        $ids = [];

        foreach ((new FilterEvaluator($filter, $schema, new Options()))->mask($batch) as $row) {
            /** @var int $id */
            $id = $batch['id'][$row];
            $ids[] = $id;
        }

        return $ids;
    }

    /**
     * The ids of the rows a read returned.
     *
     * @param iterable<array<string, mixed>> $rows
     *
     * @return list<int>
     */
    public static function ids(iterable $rows): array
    {
        $ids = [];

        foreach ($rows as $row) {
            /** @var int $id */
            $id = $row['id'];
            $ids[] = $id;
        }

        return $ids;
    }
}
