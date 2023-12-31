<?php

declare(strict_types=1);

namespace Flow\Serializer;

use Flow\ETL\Exception\RuntimeException;

final class NativePHPSerializer implements Serializer
{
    public function __construct()
    {
    }

    public function serialize(object $serializable) : string
    {
        return \serialize($serializable);
    }

    public function unserialize(string $serialized, string $class) : object
    {
        $value = \unserialize($serialized, ['allowed_classes' => true]);

        if (!\is_a($value, $class)) {
            throw new RuntimeException("NativePHPSerializer::unserialize must return instance of {$class}, got: " . $value::class);
        }

        return $value;
    }
}
