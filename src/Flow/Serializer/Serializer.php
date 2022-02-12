<?php declare(strict_types=1);

namespace Flow\Serializer;

interface Serializer
{
    public function serialize(Serializable $serializable) : string;

    public function unserialize(string $serialized) : Serializable;
}
