<?php

declare(strict_types=1);

namespace Flow\ETL\Serializer;

final class Closure
{
    private static ?bool $isSerializable = null;

    public static function isSerializable() : bool
    {
        if (self::$isSerializable === null) {
            self::$isSerializable = \class_exists('Opis\Closure\SerializableClosure');
        }

        return self::$isSerializable;
    }
}
