<?php

declare(strict_types=1);

namespace Flow\Serializer;

use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Serializer\Exception\SerializationException;

use function base64_decode;
use function base64_encode;
use function Flow\Serializer\DSL\serialize_to_string;
use function Flow\Serializer\DSL\unserialize_from_string;

final readonly class Base64Serializer implements Serializer
{
    public function __construct(
        private Serializer $serializer,
    ) {}

    public function serialize(Rows $rows, DestinationStream $destination): void
    {
        $destination->append(base64_encode(serialize_to_string($this->serializer, $rows)));
        $destination->close();
    }

    public function unserialize(SourceStream $source): Rows
    {
        $payload = $source->content();
        $source->close();

        $decoded = base64_decode($payload, true);

        if ($decoded === false) {
            throw new SerializationException('Base64Serializer::unserialize failed to decode string');
        }

        return unserialize_from_string($this->serializer, $decoded);
    }
}
