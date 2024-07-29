<?php

declare(strict_types=1);

namespace Flow\Serializer;

/**
 * @internal
 */
interface Serializer
{
    /**
     * @throw RuntimeException
     */
    public function serialize(object $serializable) : string;

    /**
     * @template T of object
     *
     * @param non-empty-array<class-string<T>> $classes
     *
     * @throw RuntimeException
     *
     * @return T
     */
    public function unserialize(string $serialized, array $classes) : object;
}
