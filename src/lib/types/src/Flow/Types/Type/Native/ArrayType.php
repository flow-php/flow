<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use DOMDocument;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\XML\XMLConverter;
use Throwable;

use function is_array;
use function is_object;
use function is_string;
use function json_decode;
use function json_encode;
use function str_starts_with;

use const JSON_THROW_ON_ERROR;

/**
 * @template T of array
 *
 * @implements Type<T>
 */
final readonly class ArrayType implements Type
{
    /**
     * @return array<array-key, mixed>
     */
    public function assert(mixed $value): array
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    /**
     * @return array<array-key, mixed>
     */
    public function cast(mixed $value): array
    {
        if ($this->isValid($value)) {
            return $value;
        }

        // hoisted above the try: the catch below re-wraps without the reason
        if (
            !is_array($value)
            && !is_object($value)
            && !(is_string($value) && (str_starts_with($value, '{') || str_starts_with($value, '[')))
        ) {
            throw new CastingException($value, $this, reason: 'wrap the value first, e.g. type_list(type_string())');
        }

        try {
            if (is_string($value) && (str_starts_with($value, '{') || str_starts_with($value, '['))) {
                // @mago-ignore analysis:mixed-assignment
                $decoded = json_decode($value, true, 512, JSON_THROW_ON_ERROR);

                return is_array($decoded) ? $decoded : throw new CastingException($value, $this);
            }

            if ($value instanceof DOMDocument) {
                return (new XMLConverter())->toArray($value);
            }

            if (is_object($value)) {
                // @mago-ignore analysis:mixed-assignment
                $encoded = json_decode(json_encode($value, JSON_THROW_ON_ERROR), true, 512, JSON_THROW_ON_ERROR);

                return is_array($encoded) ? $encoded : throw new CastingException($value, $this);
            }

            throw new CastingException($value, $this);
        } catch (Throwable) {
            throw new CastingException($value, $this);
        }
    }

    public function isValid(mixed $value): bool
    {
        if (!is_array($value)) {
            return false;
        }

        return true;
    }

    public function normalize(): array
    {
        return [
            'type' => 'array',
        ];
    }

    public function toString(): string
    {
        return 'array<mixed>';
    }
}
