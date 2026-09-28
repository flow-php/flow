<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Rows;
use ReflectionClass;

use function Flow\Types\DSL\type_instance_of;
use function serialize;
use function unserialize;

final class RowsSerializationContext
{
    public static function roundTrip(Rows $rows): Rows
    {
        return type_instance_of(Rows::class)->assert(unserialize(serialize($rows)));
    }

    /**
     * @param array{schema: \Flow\ETL\Schema, frame: string} $payload
     */
    public static function unserialize(array $payload): Rows
    {
        $rows = (new ReflectionClass(Rows::class))->newInstanceWithoutConstructor();
        $rows->__unserialize($payload);

        return $rows;
    }
}
