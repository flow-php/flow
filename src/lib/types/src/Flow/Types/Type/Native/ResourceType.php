<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\ETL\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type\Type;

/**
 * @implements Type<resource>
 */
final readonly class ResourceType implements Type
{
    public function assert(mixed $value) : mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value) : bool
    {
        return \is_resource($value);
    }

    public function normalize() : array
    {
        return [
            'type' => 'resource',
        ];
    }

    public function toString() : string
    {
        return 'resource';
    }
}
