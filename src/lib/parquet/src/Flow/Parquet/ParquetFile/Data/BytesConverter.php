<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Parquet\Exception\InvalidArgumentException;

final class BytesConverter
{
    public static function binToHex(string $binaryData, int $limit = null, string $glue = ' ') : string
    {
        if ($limit === null) {
            return \implode($glue, \str_split(\bin2hex($binaryData), 2));
        }

        return \implode($glue, \array_slice(\str_split(\bin2hex($binaryData), 2), 0, $limit));
    }

    public static function intToBin(int $number, int $bits = 32, int $bitsPerGroup = 4) : string
    {
        $bits = \max(1, $bits);

        $maxValue = 2 ** $bits - 1;

        if ($number > $maxValue) {
            throw new InvalidArgumentException(\sprintf('Number %d is too big for %d bits', $number, $bits));
        }

        $binaryString = \str_pad(\decbin($number), $bits, '0', STR_PAD_LEFT);

        if ($bitsPerGroup <= 0) {
            return $binaryString;
        }

        // Split the binary string into groups of $bitsPerGroup bits
        $splitBinary = \str_split($binaryString, $bitsPerGroup);

        // Join the groups with a space for readability
        return \implode(' ', $splitBinary);
    }

    public static function toBinary(string $bytes, int $bitsPerGroup = 8) : string
    {
        $binaryString = '';
        $length = \strlen($bytes);

        for ($i = 0; $i < $length; $i++) {
            $byte = \ord($bytes[$i]);
            $binaryString .= \str_pad(\decbin($byte), 8, '0', STR_PAD_LEFT);
        }

        if ($bitsPerGroup <= 0) {
            return $binaryString;
        }

        $splitBinary = \str_split($binaryString, $bitsPerGroup);

        return \implode(' ', $splitBinary);
    }
}
