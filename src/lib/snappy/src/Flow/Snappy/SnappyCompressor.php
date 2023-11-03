<?php declare(strict_types=1);

namespace Flow\Snappy;

/**
 * @internal This class is not meant to be used by library users. Please use Flow\Snappy\Snappy instead.
 */
final class SnappyCompressor
{
    private const BLOCK_LOG = 16;

    private const BLOCK_SIZE = 1 << self::BLOCK_LOG;

    private const MAX_HASH_TABLE_BITS = 14;

    private readonly array $array;

    private readonly int $arrayLength;

    private array $globalHashTables = [];

    public function __construct(array $uncompressed)
    {
        $this->array = $uncompressed;
        $this->arrayLength = \count($uncompressed);
    }

    public function compressToBuffer(array &$outBuffer) : int
    {
        $pos = 0;
        $outPos = 0;

        $outPos = $this->putVarInt($this->arrayLength, $outBuffer, $outPos);

        while ($pos < $this->arrayLength) {
            $fragmentSize = \min($this->arrayLength - $pos, self::BLOCK_SIZE);
            $outPos = $this->compressFragment($this->array, $pos, $fragmentSize, $outBuffer, $outPos);
            $pos += $fragmentSize;
        }

        return $outPos;
    }

    public function maxCompressedLength() : int
    {
        $sourceLen = \count($this->array);

        return 32 + $sourceLen + (int) \floor($sourceLen / 6);
    }

    private function compressFragment(array $input, int $ip, int $inputSize, array &$output, int $op) : int
    {
        $hashTableBits = 1;

        while ((1 << $hashTableBits) <= $inputSize && $hashTableBits <= self::MAX_HASH_TABLE_BITS) {
            $hashTableBits++;
        }
        $hashTableBits--;

        $hashFuncShift = 32 - $hashTableBits;

        if (!isset($this->globalHashTables[$hashTableBits])) {
            $this->globalHashTables[$hashTableBits] = \array_fill(0, 1 << $hashTableBits, 0);
        }

        $hashTable = [];

        foreach ($this->globalHashTables[$hashTableBits] as $key => $value) {
            $hashTable[$key] = 0;
        }

        $ipEnd = $ip + $inputSize;
        $baseIp = $ip;
        $nextEmit = $ip;

        $inputMargin = 15;

        if ($inputSize >= $inputMargin) {
            $ipLimit = $ipEnd - $inputMargin;

            $ip++;
            $nextHash = $this->hashFunc($this->load32($input, $ip), $hashFuncShift);

            $candidate = 0;

            while (true) {
                $skip = 32;
                $nextIp = $ip;

                do {
                    $ip = $nextIp;
                    $hash = $nextHash;
                    $bytesBetweenHashLookups = (int) ($skip / 32);
                    $skip++;
                    $nextIp = $ip + $bytesBetweenHashLookups;

                    if ($ip > $ipLimit) {
                        break 2;
                    }

                    $nextHash = $this->hashFunc($this->load32($input, $nextIp), $hashFuncShift);

                    $candidate = $baseIp + $hashTable[$hash];
                    $hashTable[$hash] = $ip - $baseIp;
                } while (!$this->equals32($input, $ip, $candidate));

                $op = $this->emitLiteral($input, $nextEmit, $ip - $nextEmit, $output, $op);

                do {
                    $base = $ip;
                    $matched = 4;

                    while ($ip + $matched < $ipEnd && $input[$ip + $matched] === $input[$candidate + $matched]) {
                        $matched++;
                    }

                    $ip += $matched;
                    $offset = $base - $candidate;
                    $op = $this->emitCopy($output, $op, $offset, $matched);

                    $nextEmit = $ip;

                    if ($ip >= $ipLimit) {
                        break 2;
                    }

                    $prevHash = $this->hashFunc($this->load32($input, $ip - 1), $hashFuncShift);
                    $hashTable[$prevHash] = $ip - 1 - $baseIp;
                    $curHash = $this->hashFunc($this->load32($input, $ip), $hashFuncShift);
                    $candidate = $baseIp + $hashTable[$curHash];
                    $hashTable[$curHash] = $ip - $baseIp;
                } while ($this->equals32($input, $ip, $candidate));

                $ip++;
                $nextHash = $this->hashFunc($this->load32($input, $ip), $hashFuncShift);
            }
        }

        if ($nextEmit < $ipEnd) {
            return $this->emitLiteral($input, $nextEmit, $ipEnd - $nextEmit, $output, $op);
        }

        return $op;
    }

    private function copyBytes(array $fromArray, int $fromPos, array &$toArray, int $toPos, int $length) : void
    {
        for ($i = 0; $i < $length; $i++) {
            $toArray[$toPos + $i] = $fromArray[$fromPos + $i];
        }
    }

    private function emitCopy(array &$output, int $op, int $offset, int $len) : int
    {
        while ($len >= 68) {
            $op = $this->emitCopyLessThan64($output, $op, $offset, 64);
            $len -= 64;
        }

        if ($len > 64) {
            $op = $this->emitCopyLessThan64($output, $op, $offset, 60);
            $len -= 60;
        }

        return $this->emitCopyLessThan64($output, $op, $offset, $len);
    }

    private function emitCopyLessThan64(array &$output, int $op, int $offset, int $len) : int
    {
        if ($len < 12 && $offset < 2048) {
            $output[$op] = 1 + (($len - 4) << 2) + (($offset >> 8) << 5);
            $output[$op + 1] = $offset & 0xFF;

            return $op + 2;
        }
        $output[$op] = 2 + (($len - 1) << 2);
        $output[$op + 1] = $offset & 0xFF;
        $output[$op + 2] = $offset >> 8;

        return $op + 3;
    }

    private function emitLiteral(array &$input, int $ip, int $len, array &$output, int $op) : int
    {
        if ($len <= 60) {
            $output[$op] = ($len - 1) << 2;
            $op++;
        } elseif ($len < 256) {
            $output[$op] = 60 << 2;
            $output[$op + 1] = $len - 1;
            $op += 2;
        } else {
            $output[$op] = 61 << 2;
            $output[$op + 1] = ($len - 1) & 0xFF;
            $output[$op + 2] = ($len - 1) >> 8;
            $op += 3;
        }
        $this->copyBytes($input, $ip, $output, $op, $len);

        return $op + $len;
    }

    private function equals32(array $array, int $pos1, int $pos2) : bool
    {
        return $array[$pos1] === $array[$pos2]
            && $array[$pos1 + 1] === $array[$pos2 + 1]
            && $array[$pos1 + 2] === $array[$pos2 + 2]
            && $array[$pos1 + 3] === $array[$pos2 + 3];
    }

    private function hashFunc(int $key, int $hashFuncShift) : int
    {
        $multiplied = $key * 0x1E35A7BD;

        // Emulate unsigned right shift in PHP
        return ($multiplied >> $hashFuncShift) & ((1 << (32 - $hashFuncShift)) - 1);
    }

    private function load32(array $array, int $pos) : int
    {
        return $array[$pos] + ($array[$pos + 1] << 8) + ($array[$pos + 2] << 16) + ($array[$pos + 3] ?? 0 << 24);
    }

    private function putVarInt(int $value, array &$output, int $op) : int
    {
        do {
            $output[$op] = $value & 0x7F;
            $value = $value >> 7;

            if ($value > 0) {
                $output[$op] += 0x80;
            }
            $op++;
        } while ($value > 0);

        return $op;
    }
}
