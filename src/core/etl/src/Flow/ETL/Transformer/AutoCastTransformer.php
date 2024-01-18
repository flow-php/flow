<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\uuid_entry;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class AutoCastTransformer implements Transformer
{
    public function autoCast(Entry $entry) : Entry
    {
        if (!$entry instanceof StringEntry) {
            return $entry;
        }

        $typeChecker = new Row\Factory\StringTypeChecker($entry->value());

        if ($typeChecker->isNull()) {
            return null_entry($entry->name());
        }

        if ($typeChecker->isInteger()) {
            return int_entry($entry->name(), (int) $entry->value());
        }

        if ($typeChecker->isFloat()) {
            return float_entry($entry->name(), (float) $entry->value());
        }

        if ($typeChecker->isBoolean()) {
            return bool_entry($entry->name(), (bool) $entry->value());
        }

        if ($typeChecker->isJson()) {
            return json_entry($entry->name(), $entry->value());
        }

        if ($typeChecker->isUuid()) {
            return uuid_entry($entry->name(), $entry->value());
        }

        if ($typeChecker->isDateTime()) {
            return datetime_entry($entry->name(), $entry->value());
        }

        return $entry;
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map(function (Row $row) {
            return $row->map(function (Entry $entry) {
                return $this->autoCast($entry);
            });
        });
    }
}
