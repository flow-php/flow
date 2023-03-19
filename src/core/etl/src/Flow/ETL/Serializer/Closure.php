<?php

declare(strict_types=1);

namespace Flow\ETL\Serializer;

use Laravel\SerializableClosure\SerializableClosure;

final class Closure
{
    private static ?bool $isSerializable = null;

    public static function isSerializable() : bool
    {
        if (self::$isSerializable === null) {
            self::$isSerializable = \class_exists(SerializableClosure::class);
        }

        return self::$isSerializable;
    }
}
