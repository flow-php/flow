<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;
use Flow\Types\Value\Uuid;

/**
 * @implements Type<Uuid>
 */
final readonly class UuidType implements Type
{
    #[\Override]
    public function assert(mixed $value) : Uuid
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    #[\Override]
    public function cast(mixed $value) : mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if ($value instanceof \DOMElement) {
            $value = $value->nodeValue;
        }

        if (\is_string($value)) {
            return new Uuid($value);
        }

        if (\is_object($value) && \is_a($value, 'Ramsey\Uuid\UuidInterface')) {
            return new Uuid($value);
        }

        if (\is_object($value) && \is_a($value, 'Symfony\Component\Uid\Uuid')) {
            return new Uuid($value->toRfc4122());
        }

        throw new CastingException($value, $this);
    }

    #[\Override]
    public function isValid(mixed $value) : bool
    {
        if ($value instanceof Uuid) {
            return true;
        }

        return false;
    }

    #[\Override]
    public function normalize() : array
    {
        return [
            'type' => 'uuid',
        ];
    }

    #[\Override]
    public function toString() : string
    {
        return 'uuid';
    }
}
