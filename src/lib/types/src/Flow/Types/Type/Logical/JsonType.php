<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Value\Json;

/**
 * @implements Type<Json>
 */
final readonly class JsonType implements Type
{
    public function assert(mixed $value): Json
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value): Json
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if ($value instanceof \DOMElement) {
            $value = $value->nodeValue;
        }

        if (\is_string($value) && Json::isValid($value)) {
            return new Json($value);
        }

        try {
            if (\is_scalar($value)) {
                throw new CastingException($value, $this);
            }

            if (\is_array($value)) {
                return Json::fromArray($value);
            }

            return new Json(\json_encode($value, \JSON_THROW_ON_ERROR));
        } catch (\Throwable $e) {
            if ($e instanceof CastingException) {
                throw $e;
            }

            throw new CastingException($value, $this);
        }
    }

    public function isValid(mixed $value): bool
    {
        return $value instanceof Json;
    }

    public function normalize(): array
    {
        return [
            'type' => 'json',
        ];
    }

    public function toString(): string
    {
        return 'json';
    }
}
