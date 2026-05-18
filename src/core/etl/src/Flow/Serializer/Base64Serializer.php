<?php

declare(strict_types=1);

namespace Flow\Serializer;

use Flow\Serializer\Exception\SerializationException;

use function base64_decode;
use function base64_encode;

final readonly class Base64Serializer implements Serializer
{
    public function __construct(
        private Serializer $serializer,
    ) {}

    public function serialize(object $serializable): string
    {
        return base64_encode($this->serializer->serialize($serializable));
    }

    public function unserialize(string $serialized, array $classes): object
    {
        $decodedString = base64_decode($serialized, true);

        if ($decodedString === false) {
            throw new SerializationException('Base64Serializer::unserialize failed to decode string');
        }

        return $this->serializer->unserialize($decodedString, $classes);
    }
}
