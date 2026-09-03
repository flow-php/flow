<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Flow\Types\Type;

use function Flow\Types\DSL\type_string;

/**
 * SQLite has no per-column type: SQLite3Result::columnType() describes the value, not the column,
 * and sqlite3_column_decltype() is not bound in PHP. An all-string schema is a true statement about
 * it rather than a floor, and the read casts values to match. This is DuckDB's sqlite_query() model.
 */
final readonly class SqliteTypes implements NativeTypes
{
    public function toFlowType(int|string|null $native): ?Type
    {
        return type_string();
    }
}
