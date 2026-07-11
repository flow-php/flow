<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use Flow\Floe\Exception\FloeException;
use Flow\Floe\Format;

use function ord;
use function sprintf;
use function substr;
use function unpack;

/**
 * Tagged decoding for values whose type is only known at runtime
 * (mixed/union/scalar/literal/array columns, dynamic map keys, structure extras).
 */
final class DynamicDecoder implements ValueDecoder
{
    public function __construct(
        private readonly DateTimeDecoder $dateTime,
        private readonly UuidDecoder $uuid,
        private readonly JsonDecoder $json,
    ) {}

    public function decode(string $data, int &$position): mixed
    {
        $tag = ord($data[$position++]);

        switch ($tag) {
            case Format::TAG_NULL:
                return null;
            case Format::TAG_INTEGER:
                $value = unpack('P', $data, $position)[1];
                $position += 8;

                return $value;
            case Format::TAG_FLOAT:
                $value = unpack('e', $data, $position)[1];
                $position += 8;

                return $value;
            case Format::TAG_BOOLEAN:
                return $data[$position++] === "\x01";
            case Format::TAG_STRING:
                $length = unpack('V', $data, $position)[1];
                $value = substr($data, $position + 4, $length);
                $position += 4 + $length;

                return $value;
            case Format::TAG_ARRAY:
                $count = unpack('V', $data, $position)[1];
                $position += 4;
                $value = [];

                for ($i = 0; $i < $count; $i++) {
                    if (ord($data[$position++]) === Format::KEY_INTEGER) {
                        $key = unpack('P', $data, $position)[1];
                        $position += 8;
                    } else {
                        $length = unpack('V', $data, $position)[1];
                        $key = substr($data, $position + 4, $length);
                        $position += 4 + $length;
                    }

                    $value[$key] = $this->decode($data, $position);
                }

                return $value;
            case Format::TAG_DATETIME:
                return $this->dateTime->decode($data, $position);
            case Format::TAG_UUID:
                return $this->uuid->decode($data, $position);
            case Format::TAG_JSON:
                return $this->json->decode($data, $position);
            default:
                throw new FloeException(sprintf('Floe found unknown dynamic value tag 0x%02X', $tag));
        }
    }
}
