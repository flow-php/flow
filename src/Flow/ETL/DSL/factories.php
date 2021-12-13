<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Factory;

use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\Cast\CastRow;
use Flow\ETL\Transformer\Factory\ArrayRowsFactory;
use Flow\ETL\Transformer\Factory\CastedRowsFactory;
use Flow\ETL\Transformer\Factory\NativeEntryFactory;

/**
 * @param array<array<mixed>> $data
 */
function rows_from_array(array $data) : Rows
{
    return (new ArrayRowsFactory())->create($data);
}

/**
 * @param array<array<mixed>> $data
 */
function rows_from_casted_array(array $data, CastRow ...$cast_rows) : Rows
{
    return (new CastedRowsFactory(new ArrayRowsFactory(), ...$cast_rows))->create($data);
}

/**
 * @param string $column
 * @param mixed $value
 */
function column_from_value(string $column, $value) : Entry
{
    return (new NativeEntryFactory())->createEntry($column, $value);
}
