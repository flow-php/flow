<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Schema;

use function array_diff_key;
use function array_flip;
use function array_keys;
use function is_int;

final class DeclaredColumns
{
    /**
     * @param array<array<array-key, mixed>> $rows
     *
     * @return list<array<array-key, mixed>>
     */
    public function project(array $rows, Schema $schema): array
    {
        $declared = array_flip($schema->references()->names());
        $projected = [];

        foreach ($rows as $row) {
            foreach (array_keys(array_diff_key($row, $declared)) as $key) {
                if (!is_int($key) || $schema->findDefinition((new ColumnName())->of($key)) === null) {
                    unset($row[$key]);
                }
            }

            $projected[] = $row;
        }

        return $projected;
    }
}
