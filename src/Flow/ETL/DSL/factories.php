<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Factory;

use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\Cast\CastEntry;
use Flow\ETL\Transformer\Factory\ArrayRowsFactory;
use Flow\ETL\Transformer\Factory\CastedRowsFactory;
use Flow\ETL\Transformer\Factory\NativeEntryFactory;

function rows_from_array(array $data) : Rows
{
    return (new ArrayRowsFactory())->create($data);
}

function rows_from_casted_array(array $data, CastEntry ...$cast_entries) : Rows
{
    return (new CastedRowsFactory(new ArrayRowsFactory(), ...$cast_entries))->create($data);
}

function column_from_value(string $column, $value) : Entry
{
    return (new NativeEntryFactory())->createEntry($column, $value);
}
