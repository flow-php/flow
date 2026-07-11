<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Floe\Exception\FloeException;
use Flow\Serializer\Exception\SerializationException;
use Flow\Serializer\Serializer;

use function get_debug_type;
use function implode;
use function is_a;
use function sprintf;

final class FloeSerializer implements Serializer
{
    private readonly FloeValueSerializer $serializer;

    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(int $batchSize = 1000, ?bool $useExtension = null)
    {
        $this->serializer = new FloeValueSerializer($batchSize, $useExtension);
    }

    public function serialize(object $serializable): string
    {
        if (!$serializable instanceof Rows && !$serializable instanceof Row) {
            throw new SerializationException(sprintf(
                'FloeSerializer supports only Rows and Row, got: %s',
                get_debug_type($serializable),
            ));
        }

        return $this->serializer->encode($serializable);
    }

    public function unserialize(string $serialized, array $classes): object
    {
        try {
            $value = $this->serializer->decode($serialized);
        } catch (FloeException $e) {
            throw new SerializationException($e->getMessage(), 0, $e);
        }

        foreach ($classes as $class) {
            if (is_a($value, $class)) {
                return $value;
            }
        }

        throw new SerializationException(sprintf(
            'FloeSerializer::unserialize must return instance of {%s}, got: %s',
            implode(', ', $classes),
            get_debug_type($value),
        ));
    }
}
