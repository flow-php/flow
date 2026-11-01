<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\IO\Extractor;

use Flow\Types\Type;

use function Flow\Types\DSL\type_string;

/**
 * SQLite has no per-column type in PHP, so an all-string schema is true, not a floor.
 */
final readonly class SqliteTypes implements NativeTypes
{
    public function toFlowType(int|string|null $native): ?Type
    {
        return type_string();
    }
}
