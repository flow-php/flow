<?php declare(strict_types=1);

namespace Flow\Serializer;

use Flow\ETL\Exception\RuntimeException;

/**
 * @internal
 */
interface Serializer
{
    /**
     * @throw RuntimeException
     */
    public function serialize(Serializable $serializable) : string;

    /**
     * @param class-string $class
     *
     * @throw RuntimeException
     */
    public function unserialize(string $serialized, string $class) : Serializable;
}
