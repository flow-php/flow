<?php

declare(strict_types=1);

namespace Flow\Serializer;

final readonly class Base64Serializer implements Serializer
{
    public function __construct(private Serializer $serializer)
    {
    }

    public function serialize(object $serializable) : string
    {
        return \base64_encode($this->serializer->serialize($serializable));
    }

    public function unserialize(string $serialized, array $classes) : object
    {
        /** @phpstan-ignore-next-line */
        return $this->serializer->unserialize(\base64_decode($serialized, true), $classes);
    }
}
