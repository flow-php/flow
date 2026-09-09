<?php

declare(strict_types=1);

namespace Flow\Parquet\Binary;

use OverflowException;

use function abs;
use function array_reverse;
use function array_values;
use function bccomp;
use function bcdiv;
use function bcpow;
use function count;
use function max;
use function number_format;
use function pack;
use function sprintf;
use function str_repeat;
use function strlen;
use function unpack;

/**
 * @param array<int> $values
 */
function encode_i8(array $values): string
{
    if (count($values) === 0) {
        return '';
    }

    return pack(str_repeat('c', count($values)), ...$values);
}

/**
 * @return array<int>
 */
function decode_i8(string $bytes): array
{
    /** @var array<int, int> $values */
    $values = unpack('c*', $bytes);

    return array_values($values);
}

/**
 * @param array<int> $values
 */
function encode_i16(ByteOrder $order, array $values): string
{
    if (count($values) === 0) {
        return '';
    }

    $format = $order === ByteOrder::BIG_ENDIAN ? 'n' : 'v';

    return pack(str_repeat($format, count($values)), ...$values);
}

/**
 * @return array<int>
 */
function decode_i16(ByteOrder $order, string $bytes): array
{
    $format = $order === ByteOrder::LITTLE_ENDIAN ? 'v' : 'n';
    /** @var array<int, int> $values */
    $values = unpack($format . '*', $bytes);

    foreach ($values as $k => $v) {
        if ($v & 0x8000) {
            $values[$k] = -((~$v & 0xFFFF) + 1);
        }
    }

    return array_values($values);
}

/**
 * @param array<int> $values
 */
function encode_i32(ByteOrder $order, array $values): string
{
    if (count($values) === 0) {
        return '';
    }

    $format = $order === ByteOrder::BIG_ENDIAN ? 'N' : 'V';

    return pack(str_repeat($format, count($values)), ...$values);
}

/**
 * @return array<int>
 */
function decode_i32(ByteOrder $order, string $bytes): array
{
    $format = $order === ByteOrder::LITTLE_ENDIAN ? 'V' : 'N';
    /** @var array<int, int> $values */
    $values = unpack($format . '*', $bytes);

    foreach ($values as $k => $v) {
        if ($v >= 0x80000000) {
            $values[$k] = $v - 0x100000000;
        }
    }

    return array_values($values);
}

/**
 * @param array<int> $values
 */
function encode_i64(ByteOrder $order, array $values): string
{
    if (count($values) === 0) {
        return '';
    }

    $format = $order === ByteOrder::BIG_ENDIAN ? 'J' : 'P';

    return pack(str_repeat($format, count($values)), ...$values);
}

/**
 * @return array<int>
 */
function decode_i64(ByteOrder $order, string $bytes): array
{
    $format = $order === ByteOrder::LITTLE_ENDIAN ? 'P' : 'J';
    /** @var array<int, int> $values */
    $values = unpack($format . '*', $bytes);

    return array_values($values);
}

/**
 * @param array<int> $values
 */
function encode_u8(array $values): string
{
    if (count($values) === 0) {
        return '';
    }

    return pack(str_repeat('C', count($values)), ...$values);
}

/**
 * @return array<int>
 */
function decode_u8(string $bytes): array
{
    /** @var array<int, int> $values */
    $values = unpack('C*', $bytes);

    return array_values($values);
}

/**
 * @param array<int> $values
 */
function encode_u16(ByteOrder $order, array $values): string
{
    if (count($values) === 0) {
        return '';
    }

    $format = $order === ByteOrder::BIG_ENDIAN ? 'n' : 'v';

    return pack(str_repeat($format, count($values)), ...$values);
}

/**
 * @return array<int>
 */
function decode_u16(ByteOrder $order, string $bytes): array
{
    $format = $order === ByteOrder::LITTLE_ENDIAN ? 'v' : 'n';
    /** @var array<int, int> $values */
    $values = unpack($format . '*', $bytes);

    return array_values($values);
}

/**
 * @param array<int> $values
 */
function encode_u32(ByteOrder $order, array $values): string
{
    if (count($values) === 0) {
        return '';
    }

    $format = $order === ByteOrder::BIG_ENDIAN ? 'N' : 'V';

    return pack(str_repeat($format, count($values)), ...$values);
}

/**
 * @return array<int>
 */
function decode_u32(ByteOrder $order, string $bytes): array
{
    $format = $order === ByteOrder::LITTLE_ENDIAN ? 'V' : 'N';
    /** @var array<int, int> $values */
    $values = unpack($format . '*', $bytes);

    return array_values($values);
}

/**
 * @param array<int> $values
 */
function encode_u64(ByteOrder $order, array $values): string
{
    if (count($values) === 0) {
        return '';
    }

    $format = $order === ByteOrder::BIG_ENDIAN ? 'J' : 'P';

    return pack(str_repeat($format, count($values)), ...$values);
}

/**
 * @return array<int>
 */
function decode_u64(ByteOrder $order, string $bytes): array
{
    $format = $order === ByteOrder::LITTLE_ENDIAN ? 'P' : 'J';
    /** @var array<int, int> $values */
    $values = unpack($format . '*', $bytes);

    return array_values($values);
}

/**
 * @param array<float> $values
 */
function encode_f32(ByteOrder $order, array $values): string
{
    if (count($values) === 0) {
        return '';
    }

    $format = $order === ByteOrder::BIG_ENDIAN ? 'G' : 'g';

    return pack(str_repeat($format, count($values)), ...$values);
}

/**
 * @return array<float>
 */
function decode_f32(ByteOrder $order, string $bytes): array
{
    $format = $order === ByteOrder::LITTLE_ENDIAN ? 'g' : 'G';
    /** @var array<int, float> $values */
    $values = unpack($format . '*', $bytes);

    return array_values($values);
}

/**
 * @param array<float> $values
 */
function encode_f64(ByteOrder $order, array $values): string
{
    if (count($values) === 0) {
        return '';
    }

    $format = $order === ByteOrder::BIG_ENDIAN ? 'E' : 'e';

    return pack(str_repeat($format, count($values)), ...$values);
}

/**
 * @return array<float>
 */
function decode_f64(ByteOrder $order, string $bytes): array
{
    $format = $order === ByteOrder::LITTLE_ENDIAN ? 'e' : 'E';
    /** @var array<int, float> $values */
    $values = unpack($format . '*', $bytes);

    return array_values($values);
}

function encode_decimal(ByteOrder $order, float $value, int $byteLength, int $precision, int $scale): string
{
    $decimalInt = (int) number_format($value, $scale, '', '');

    $maxUnscaled = bcpow('10', (string) $precision);

    if (bccomp((string) abs($decimalInt), $maxUnscaled) >= 0) {
        throw new OverflowException(sprintf(
            'Decimal value %s exceeds maximum precision of %d digits',
            $value,
            $precision,
        ));
    }

    $bytes = [];

    for ($i = $byteLength - 1; $i >= 0; $i--) {
        $shift = $i * 8;
        $bytes[] = ($decimalInt >> $shift) & 0xFF;
    }

    if ($order === ByteOrder::BIG_ENDIAN) {
        $bytes = array_reverse($bytes);
    }

    $packedBytes = '';

    foreach ($bytes as $byte) {
        $packedBytes .= pack('C', $byte);
    }

    return $packedBytes;
}

function decode_decimal(ByteOrder $order, string $bytes, int $precision, int $scale): float
{
    $byteLength = strlen($bytes);
    $intValue = 0;

    /** @var array<int, int> $byteArray */
    $byteArray = unpack('C*', $bytes);

    if ($order === ByteOrder::BIG_ENDIAN) {
        $byteArray = array_values(array_reverse($byteArray));

        foreach ($byteArray as $i => $byte) {
            $shift = ($byteLength - $i - 1) * 8;
            $intValue |= $byte << $shift;
        }
    } else {
        foreach ($byteArray as $i => $byte) {
            $shift = ($byteLength - $i) * 8;
            $intValue |= $byte << $shift;
        }
    }

    $maxUnscaled = bcpow('10', (string) $precision);

    if (bccomp((string) abs($intValue), $maxUnscaled) >= 0) {
        throw new OverflowException(sprintf(
            'Decoded decimal value %d exceeds maximum precision of %d digits',
            $intValue,
            $precision,
        ));
    }

    return (float) bcdiv((string) $intValue, bcpow('10', (string) $scale), max(0, $scale));
}
