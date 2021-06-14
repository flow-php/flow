<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Factory;

use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\Cast\CastEntry;
use Flow\ETL\Transformer\Factory\ArrayRowsFactory;
use Flow\ETL\Transformer\Factory\CastedRowsFactory;
use Flow\ETL\Transformer\Factory\NativeEntryFactory;

function rowsFromArray(array $data) : Rows
{
    return (new ArrayRowsFactory())->create($data);
}

function rowsFromCastedArray(array $data, CastEntry ...$castEntries) : Rows
{
    return (new CastedRowsFactory(new ArrayRowsFactory(), ...$castEntries))->create($data);
}

function columnFromValue(string $column, $value) : Entry
{
    return (new NativeEntryFactory())->createEntry($column, $value);
}
