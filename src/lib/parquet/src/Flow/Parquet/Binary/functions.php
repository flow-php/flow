<?php

declare(strict_types=1);

namespace Flow\Parquet\Binary;

use Flow\Parquet\Exception\InvalidArgumentException;
use OverflowException;

use function abs;
use function array_values;
use function bcadd;
use function bccomp;
use function bcdiv;
use function bcmod;
use function bcmul;
use function bcpow;
use function bcsub;
use function chr;
use function count;
use function explode;
use function is_numeric;
use function ltrim;
use function max;
use function ord;
use function pack;
use function sprintf;
use function str_pad;
use function str_repeat;
use function str_replace;
use function str_starts_with;
use function strlen;
use function strpos;
use function substr;
use function unpack;
use function var_export;

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

/**
 * Rounds the shortest round-trip representation of the float half away from zero - the same digits arrow-ext writes.
 *
 * @return numeric-string
 */
function decimal_unscaled(float $value, int $precision, int $scale): string
{
    $repr = var_export($value, true);
    $parts = explode('E', $repr);
    $mantissa = ltrim($parts[0], '-');
    $dot = strpos($mantissa, '.');
    $digits = str_replace('.', '', $mantissa);
    $point = ($dot === false ? strlen($mantissa) : $dot) + (int) ($parts[1] ?? 0);
    $scale = max(0, $scale);

    $plain = (str_starts_with($repr, '-') ? '-' : '') . match (true) {
        $point <= 0 => '0.' . str_repeat('0', abs($point)) . $digits,
        $point >= strlen($digits) => str_pad($digits, $point, '0'),
        default => substr($digits, 0, $point) . '.' . substr($digits, $point),
    };

    if (!is_numeric($plain)) {
        throw new InvalidArgumentException(sprintf('Decimal value %s is not a finite number', $repr));
    }

    $half = bcdiv('5', bcpow('10', (string) ($scale + 1)), $scale + 1);
    $unscaled = bcmul(
        bcadd($plain, str_starts_with($plain, '-') ? bcsub('0', $half, $scale + 1) : $half, $scale),
        bcpow('10', (string) $scale),
        0,
    );

    if (strlen(ltrim($unscaled, '-')) > $precision) {
        throw new OverflowException(sprintf(
            'Decimal value %s exceeds maximum precision of %d digits',
            $unscaled,
            $precision,
        ));
    }

    return $unscaled;
}

/**
 * @param numeric-string $unscaled
 */
function decimal_from_unscaled(string $unscaled, int $scale): float
{
    $scale = max(0, $scale);

    return (float) bcdiv($unscaled, bcpow('10', (string) $scale), $scale);
}

/**
 * Decimals are big-endian two's complement. A null byte length encodes the minimal number of bytes (BYTE_ARRAY).
 */
function encode_decimal(float $value, int $precision, int $scale, ?int $byteLength): string
{
    $unscaled = decimal_unscaled($value, $precision, $scale);
    $negative = str_starts_with($unscaled, '-');

    if (strlen(ltrim($unscaled, '-')) <= 18) {
        $bytes = pack('J', (int) $unscaled);

        if ($byteLength === null) {
            $signByte = $negative ? "\xFF" : "\x00";

            while (strlen($bytes) > 1 && $bytes[0] === $signByte && ord($bytes[1]) >= 0x80 === $negative) {
                $bytes = substr($bytes, 1);
            }

            return $bytes;
        }

        return $byteLength <= 8
            ? substr($bytes, -$byteLength)
            : str_pad($bytes, $byteLength, $negative ? "\xFF" : "\x00", STR_PAD_LEFT);
    }

    if ($byteLength === null) {
        $byteLength = 1;

        while (
            bccomp($unscaled, bcpow('2', (string) ((8 * $byteLength) - 1))) >= 0
            || bccomp($unscaled, bcsub('0', bcpow('2', (string) ((8 * $byteLength) - 1)))) < 0
        ) {
            $byteLength++;
        }
    }

    $unsigned = $negative ? bcadd(bcpow('2', (string) (8 * $byteLength)), $unscaled) : $unscaled;
    $bytes = '';

    for ($i = 0; $i < $byteLength; $i++) {
        $bytes = chr((int) bcmod($unsigned, '256')) . $bytes;
        $unsigned = bcdiv($unsigned, '256', 0);
    }

    return $bytes;
}

function decode_decimal(string $bytes, int $scale): float
{
    if ($bytes === '') {
        return 0.0;
    }

    $byteLength = strlen($bytes);
    $negative = ord($bytes[0]) >= 0x80;

    if ($byteLength <= 8) {
        /** @var array{1: int} $unpacked */
        $unpacked = unpack('J', str_pad($bytes, 8, $negative ? "\xFF" : "\x00", STR_PAD_LEFT));

        return decimal_from_unscaled((string) $unpacked[1], $scale);
    }

    $unscaled = '0';

    for ($i = 0; $i < $byteLength; $i++) {
        $unscaled = bcadd(bcmul($unscaled, '256'), (string) ord($bytes[$i]));
    }

    return decimal_from_unscaled(
        $negative ? bcsub($unscaled, bcpow('2', (string) (8 * $byteLength))) : $unscaled,
        $scale,
    );
}
