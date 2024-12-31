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

    public function unserialize(string $serialized, array $classes) : object
    {
        $value = \unserialize($serialized, ['allowed_classes' => true]);

        foreach ($classes as $class) {
            if (\is_a($value, $class)) {
                return $value;
            }
        }

        throw new RuntimeException(\sprintf('NativePHPSerializer::unserialize must return instance of {%s}, got: %s', \implode(', ', $classes), $value::class));
    }
}
