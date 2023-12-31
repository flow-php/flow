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

    public function unserialize(string $serialized) : object
    {
        $value = \unserialize($serialized, ['allowed_classes' => true]);

        if (!\is_object($value)) {
            throw new RuntimeException('NativePHPSerializer::unserialize must return object instance, got: ' . \gettype($value));
        }

        return $value;
    }
}
