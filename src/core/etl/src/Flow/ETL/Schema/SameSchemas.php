<?php

declare(strict_types=1);

namespace Flow\ETL\Schema;

use Flow\ETL\Schema;
use WeakMap;

/**
 * Pairs of schemas already proved the same. A schema never changes after construction, so a proof holds for as long
 * as both live - batches of one stream compare their equal-but-distinct schemas once, not once per batch.
 */
final class SameSchemas
{
    /**
     * @var null|WeakMap<Schema, WeakMap<Schema, true>>
     */
    private static ?WeakMap $proved = null;

    public static function proved(Schema $schema, Schema $other): bool
    {
        return (
            self::$proved !== null
            && self::$proved->offsetExists($schema)
            && self::$proved[$schema]->offsetExists($other)
        );
    }

    public static function remember(Schema $schema, Schema $other): void
    {
        self::$proved ??= new WeakMap();
        /** @var WeakMap<Schema, true> $others */
        $others = self::$proved->offsetExists($schema) ? self::$proved[$schema] : new WeakMap();
        $others[$other] = true;
        self::$proved[$schema] = $others;
    }
}
