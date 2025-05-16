<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use function Flow\Types\DSL\type_json;
use Flow\ETL\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type\Native\String\StringTypeChecker;
use Flow\Types\Type\Type;

/**
 * @implements Type<string>
 */
final readonly class JsonType implements Type
{
    public function assert(mixed $value) : string
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : string
    {
        if ($this->isValid($value)) {
            return $value;
        }

        try {
            if (\is_scalar($value)) {
                throw new CastingException($value, type_json());
            }

            return \json_encode($value, \JSON_THROW_ON_ERROR);
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }
    }

    public function isValid(mixed $value) : bool
    {
        if (!\is_string($value)) {
            return false;
        }

        return (new StringTypeChecker($value))->isJson();
    }

    public function normalize() : array
    {
        return [
            'type' => 'json',
        ];
    }

    public function toString() : string
    {
        return 'json';
    }
}
