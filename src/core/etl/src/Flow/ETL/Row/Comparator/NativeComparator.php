<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Comparator;

use Flow\ETL\Row;
use Flow\ETL\Row\Comparator;
use Flow\ETL\Row\TypedValueComparator;
use Flow\ETL\Schema;

use function array_keys;

final class NativeComparator implements Comparator
{
    public function equals(Row $row, Row $nextRow, Schema $schema): bool
    {
        if (array_keys($row->values()) !== array_keys($nextRow->values())) {
            return false;
        }

        $comparator = new TypedValueComparator();

        foreach ($schema->definitions() as $definition) {
            $name = $definition->entry()->name();

            if (!$row->has($name) || !$nextRow->has($name)) {
                continue;
            }

            if (!$comparator->equals($definition->type(), $row->get($name), $nextRow->get($name))) {
                return false;
            }
        }

        return true;
    }
}
