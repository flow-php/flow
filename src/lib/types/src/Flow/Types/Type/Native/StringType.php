<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use DateInterval;
use DateTimeInterface;
use DateTimeZone;
use Dom\HTMLDocument;
use Dom\HTMLElement;
use DOMDocument;
use DOMElement;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Stringable;
use Throwable;

use function Flow\Types\DSL\dom_element_to_string;
use function is_array;
use function is_bool;
use function is_object;
use function is_scalar;
use function is_string;
use function json_encode;
use function round;
use function sprintf;

/**
 * @template T of string
 *
 * @implements Type<T>
 */
final readonly class StringType implements Type
{
    public function assert(mixed $value): string
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value): string
    {
        if ($this->isValid($value)) {
            return $value;
        }

        try {
            if (is_bool($value)) {
                return $value ? 'true' : 'false';
            }

            if (is_array($value)) {
                return json_encode($value, JSON_THROW_ON_ERROR);
            }

            if ($value instanceof DateTimeInterface) {
                return $value->format(DateTimeInterface::RFC3339);
            }

            if ($value instanceof Stringable) {
                return (string) $value;
            }

            if ($value instanceof DateTimeZone) {
                return $value->getName();
            }

            if ($value instanceof DateInterval) {
                $hours = ($value->d * 24) + $value->h;
                $fraction = (int) round($value->f * 1_000_000);

                return $fraction === 0
                    ? sprintf('%02d:%02d:%02d', $hours, $value->i, $value->s)
                    : sprintf('%02d:%02d:%02d.%06d', $hours, $value->i, $value->s, $fraction);
            }

            if ($value instanceof DOMDocument) {
                return $value->saveXML($value->documentElement) ?: '';
            }

            if ($value instanceof DOMElement) {
                return (string) dom_element_to_string($value);
            }

            if ($value instanceof HTMLDocument) {
                // @mago-expect analysis:unavailable-method
                return $value->saveHtml();
            }

            if ($value instanceof HTMLElement) {
                return $value->innerHTML;
            }

            if (is_scalar($value) || is_object($value) && method_exists($value, '__toString')) {
                return (string) $value;
            }

            throw new CastingException($value, $this);
        } catch (Throwable) {
            throw new CastingException($value, $this);
        }
    }

    public function isStringable(mixed $value): bool
    {
        return (
            is_string($value)
            || is_object($value) && method_exists($value, '__toString')
            || $value instanceof Stringable
        );
    }

    public function isValid(mixed $value): bool
    {
        return is_string($value);
    }

    public function normalize(): array
    {
        return [
            'type' => 'string',
        ];
    }

    public function toString(): string
    {
        return 'string';
    }
}
