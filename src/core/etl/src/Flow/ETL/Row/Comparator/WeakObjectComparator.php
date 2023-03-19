<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Comparator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Comparator;

final class WeakObjectComparator implements Comparator
{
    public function equals(Row $row, Row $nextRow) : bool
    {
        try {
            foreach ($row->entries()->all() as $entry) {
                if ($entry instanceof Row\Entry\ObjectEntry) {
                    if ($entry->value() != $nextRow->get($entry->name())->value()) {
                        return false;
                    }
                } else {
                    if (!$entry->isEqual($nextRow->get($entry->name()))) {
                        return false;
                    }
                }
            }

            return true;
        } catch (InvalidArgumentException) {
            return false;
        }
    }
}
