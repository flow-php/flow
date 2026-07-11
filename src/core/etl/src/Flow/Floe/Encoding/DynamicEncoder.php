<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use DateTimeInterface;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Format;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;

use function chr;
use function count;
use function get_debug_type;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function pack;
use function sprintf;
use function strlen;

/**
 * Tagged encoding for values whose type is only known at runtime
 * (mixed/union/scalar/literal/array columns, dynamic map keys, structure extras).
 */
final class DynamicEncoder implements ValueEncoder
{
    public function __construct(
        private readonly DateTimeEncoder $dateTime = new DateTimeEncoder(),
    ) {}

    public function encode(mixed $value): string
    {
        if ($value === null) {
            return chr(Format::TAG_NULL);
        }

        if (is_int($value)) {
            return chr(Format::TAG_INTEGER) . pack('P', $value);
        }

        if (is_float($value)) {
            return chr(Format::TAG_FLOAT) . pack('e', $value);
        }

        if (is_bool($value)) {
            return chr(Format::TAG_BOOLEAN) . ($value ? "\x01" : "\x00");
        }

        if (is_string($value)) {
            return chr(Format::TAG_STRING) . pack('V', strlen($value)) . $value;
        }

        if (is_array($value)) {
            $buffer = chr(Format::TAG_ARRAY) . pack('V', count($value));

            // @mago-ignore analysis:mixed-assignment
            foreach ($value as $key => $item) {
                $buffer .= is_int($key)
                    ? chr(Format::KEY_INTEGER) . pack('P', $key)
                    : chr(Format::KEY_STRING) . pack('V', strlen($key)) . $key;
                $buffer .= $this->encode($item);
            }

            return $buffer;
        }

        if ($value instanceof DateTimeInterface) {
            return chr(Format::TAG_DATETIME) . $this->dateTime->encode($value);
        }

        if ($value instanceof Uuid) {
            return chr(Format::TAG_UUID) . $value->toString();
        }

        if ($value instanceof Json) {
            $json = $value->toString();

            return chr(Format::TAG_JSON) . pack('V', strlen($json)) . $json . ($value->isObject() ? "\x01" : "\x00");
        }

        throw new FloeException(sprintf(
            'Floe does not support values of type "%s" in mixed/union context',
            get_debug_type($value),
        ));
    }
}
