<?php

declare(strict_types=1);

namespace Flow\Serializer;

use Flow\ETL\Exception\RuntimeException;

use function implode;
use function is_a;
use function is_object;
use function serialize;
use function sprintf;
use function unserialize;

final class NativePHPSerializer implements Serializer
{
    public function __construct() {}

    public function serialize(object $serializable): string
    {
        return serialize($serializable);
    }

    public function unserialize(string $serialized, array $classes): object
    {
        $value = unserialize($serialized, ['allowed_classes' => true]);

        foreach ($classes as $class) {
            if (is_object($value) && is_a($value, $class)) {
                return $value;
            }
        }

        throw new RuntimeException(sprintf(
            'NativePHPSerializer::unserialize must return instance of {%s}, got: %s',
            implode(', ', $classes),
            get_debug_type($value),
        ));
    }
}
