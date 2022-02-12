<?php

declare(strict_types=1);

namespace Flow\Serializer;

final class NativePHPSerializer implements Serializer
{
    public function serialize(Serializable $serializable) : string
    {
        /**
         * @psalm-suppress MixedInferredReturnType
         */
        return \serialize($serializable);
    }

    /**
     * @psalm-suppress MixedInferredReturnType
     */
    public function unserialize(string $serialized) : Serializable
    {
        /**
         * @psalm-suppress MixedReturnStatement
         * @phpstan-ignore-next-line
         */
        return \unserialize($serialized);
    }
}
