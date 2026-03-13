<?php

declare(strict_types=1);

namespace Flow\Parquet\Binary;

function encode_i8(int $value) : string
{
    return \pack('c', $value);
}

function decode_i8(string $bytes) : int
{
    $value = \ord($bytes[0]);

    if ($value & 0x80) {
        $value = -((~$value & 0xFF) + 1);
    }

    return $value;
}

function encode_i16(ByteOrder $order, int $value) : string
{
    $format = $order === ByteOrder::BIG_ENDIAN ? 'n' : 'v';

    return \pack($format, $value);
}

function decode_i16(ByteOrder $order, string $bytes) : int
{
    /** @var array<int, int> $byteArray */
    $byteArray = \unpack('C*', $bytes);

    if ($order === ByteOrder::LITTLE_ENDIAN) {
        $integer = $byteArray[1] | ($byteArray[2] << 8);
    } else {
        $integer = ($byteArray[1] << 8) | $byteArray[2];
    }

    if ($integer & 0x8000) {
        $integer = -((~$integer & 0xFFFF) + 1);
    }

    return $integer;
}

function encode_i32(ByteOrder $order, int $value) : string
{
    $format = $order === ByteOrder::BIG_ENDIAN ? 'N' : 'V';

    return \pack($format, $value);
}

function decode_i32(ByteOrder $order, string $bytes) : int
{
    /** @var array<int, int> $byteArray */
    $byteArray = \unpack('C*', $bytes);

    if ($order === ByteOrder::LITTLE_ENDIAN) {
        $int = $byteArray[1] | ($byteArray[2] << 8) | ($byteArray[3] << 16) | ($byteArray[4] << 24);
    } else {
        $int = ($byteArray[1] << 24) | ($byteArray[2] << 16) | ($byteArray[3] << 8) | $byteArray[4];
    }

    if ($int & 0x80000000) {
        $int = -((~$int & 0xFFFFFFFF) + 1);
    }

    return $int;
}

function encode_i64(ByteOrder $order, int $value) : string
{
    $format = $order === ByteOrder::BIG_ENDIAN ? 'J' : 'P';

    return \pack($format, $value);
}

function decode_i64(ByteOrder $order, string $bytes) : int
{
    /** @var array<int, int> $byteArray */
    $byteArray = \unpack('C*', $bytes);

    if ($order === ByteOrder::LITTLE_ENDIAN) {
        $int = $byteArray[1] | ($byteArray[2] << 8) | ($byteArray[3] << 16) | ($byteArray[4] << 24) |
            ($byteArray[5] << 32) | ($byteArray[6] << 40) | ($byteArray[7] << 48) | ($byteArray[8] << 56);
        $sign = $byteArray[8];
    } else {
        $int = ($byteArray[1] << 56) | ($byteArray[2] << 48) | ($byteArray[3] << 40) | ($byteArray[4] << 32) |
            ($byteArray[5] << 24) | ($byteArray[6] << 16) | ($byteArray[7] << 8) | $byteArray[8];
        $sign = $byteArray[1];
    }

    if ($sign & 0x80) {
        $int |= (-1 ^ 0xFFFFFFFFFFFFFFFF) << 56;
    } else {
        $int |= $sign << 56;
    }

    return $int;
}

function encode_u8(int $value) : string
{
    return \pack('C', $value);
}

function decode_u8(string $bytes) : int
{
    return \ord($bytes[0]);
}

function encode_u16(ByteOrder $order, int $value) : string
{
    $format = $order === ByteOrder::BIG_ENDIAN ? 'n' : 'v';

    return \pack($format, $value);
}

function decode_u16(ByteOrder $order, string $bytes) : int
{
    $format = $order === ByteOrder::LITTLE_ENDIAN ? 'v' : 'n';

    /** @var array<int, int> $result */
    $result = \unpack($format, $bytes);

    return $result[1];
}

function encode_u32(ByteOrder $order, int $value) : string
{
    $format = $order === ByteOrder::BIG_ENDIAN ? 'N' : 'V';

    return \pack($format, $value);
}

function decode_u32(ByteOrder $order, string $bytes) : int
{
    $format = $order === ByteOrder::LITTLE_ENDIAN ? 'V' : 'N';

    /** @var array<int, int> $result */
    $result = \unpack($format, $bytes);

    return $result[1];
}

function encode_u64(ByteOrder $order, int $value) : string
{
    $format = $order === ByteOrder::BIG_ENDIAN ? 'J' : 'P';

    return \pack($format, $value);
}

function decode_u64(ByteOrder $order, string $bytes) : int
{
    $format = $order === ByteOrder::LITTLE_ENDIAN ? 'P' : 'J';

    /** @var array<int, int> $result */
    $result = \unpack($format, $bytes);

    return $result[1];
}

function encode_f32(ByteOrder $order, float $value) : string
{
    $format = $order === ByteOrder::BIG_ENDIAN ? 'G' : 'g';

    return \pack($format, $value);
}

function decode_f32(ByteOrder $order, string $bytes) : float
{
    $format = $order === ByteOrder::LITTLE_ENDIAN ? 'g' : 'G';

    /** @var array<int, float> $result */
    $result = \unpack($format, $bytes);

    return \round($result[1], 7);
}

function encode_f64(ByteOrder $order, float $value) : string
{
    $format = $order === ByteOrder::BIG_ENDIAN ? 'E' : 'e';

    return \pack($format, $value);
}

function decode_f64(ByteOrder $order, string $bytes) : float
{
    $format = $order === ByteOrder::LITTLE_ENDIAN ? 'e' : 'E';

    /** @var array<int, float> $result */
    $result = \unpack($format, $bytes);

    return $result[1];
}

function encode_decimal(ByteOrder $order, float $value, int $byteLength, int $precision, int $scale) : string
{
    $decimalInt = (int) \number_format($value, $scale, '', '');

    $maxUnscaled = \bcpow('10', (string) $precision);

    if (\bccomp((string) \abs($decimalInt), $maxUnscaled) >= 0) {
        throw new \OverflowException(\sprintf(
            'Decimal value %s exceeds maximum precision of %d digits',
            $value,
            $precision
        ));
    }

    $bytes = [];

    for ($i = $byteLength - 1; $i >= 0; $i--) {
        $shift = $i * 8;
        $bytes[] = ($decimalInt >> $shift) & 0xFF;
    }

    if ($order === ByteOrder::BIG_ENDIAN) {
        $bytes = \array_reverse($bytes);
    }

    $packedBytes = '';

    foreach ($bytes as $byte) {
        $packedBytes .= \pack('C', $byte);
    }

    return $packedBytes;
}

function decode_decimal(ByteOrder $order, string $bytes, int $precision, int $scale) : float
{
    $byteLength = \strlen($bytes);
    $intValue = 0;

    /** @var array<int, int> $byteArray */
    $byteArray = \unpack('C*', $bytes);

    if ($order === ByteOrder::BIG_ENDIAN) {
        $byteArray = \array_values(\array_reverse($byteArray));

        foreach ($byteArray as $i => $byte) {
            $shift = ($byteLength - $i - 1) * 8;
            $intValue |= ($byte << $shift);
        }
    } else {
        foreach ($byteArray as $i => $byte) {
            $shift = ($byteLength - $i) * 8;
            $intValue |= ($byte << $shift);
        }
    }

    $maxUnscaled = \bcpow('10', (string) $precision);

    if (\bccomp((string) \abs($intValue), $maxUnscaled) >= 0) {
        throw new \OverflowException(\sprintf(
            'Decoded decimal value %d exceeds maximum precision of %d digits',
            $intValue,
            $precision
        ));
    }

    return (float) \bcdiv((string) $intValue, \bcpow('10', (string) $scale), $scale);
}
