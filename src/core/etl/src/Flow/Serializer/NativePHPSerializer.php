<?php

declare(strict_types=1);

namespace Flow\Serializer;

use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Serializer\Exception\SerializationException;

use function get_debug_type;
use function serialize;
use function sprintf;
use function unserialize;

final class NativePHPSerializer implements Serializer
{
    public function __construct() {}

    public function serialize(Rows $rows, DestinationStream $destination): void
    {
        $destination->append(serialize($rows));
        $destination->close();
    }

    public function unserialize(SourceStream $source): Rows
    {
        $payload = $source->content();
        $source->close();

        // @mago-ignore analysis:mixed-assignment
        $value = unserialize($payload, ['allowed_classes' => true]);

        if (!$value instanceof Rows) {
            throw new SerializationException(sprintf(
                'NativePHPSerializer::unserialize must return instance of %s, got: %s',
                Rows::class,
                get_debug_type($value),
            ));
        }

        return $value;
    }
}
