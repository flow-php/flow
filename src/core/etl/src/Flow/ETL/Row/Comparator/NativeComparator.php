<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Comparator;

use Flow\ETL\Row;
use Flow\ETL\Row\Comparator;
use Flow\ETL\Row\TypedValueComparator;
use Flow\ETL\Schema;

final class NativeComparator implements Comparator
{
    public function equals(Row $row, Row $nextRow, Schema $schema): bool
    {
        if ($row->names() !== $nextRow->names()) {
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
