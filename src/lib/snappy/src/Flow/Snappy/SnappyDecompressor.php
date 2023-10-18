<?php declare(strict_types=1);

namespace Flow\Snappy;

/**
 * @internal This class is not meant to be used by library users. Please use Flow\Snappy\Snappy instead.
 */
final class SnappyDecompressor
{
    private const WORD_MASK = [0, 0xff, 0xffff, 0xffffff, 0xffffffff];

    private array $array;

    private int $pos;

    public function __construct(array $compressed)
    {
        $this->array = $compressed;
        $this->pos = 0;
    }

    public function readUncompressedLength() : int
    {
        $result = 0;
        $shift = 0;

        while ($shift < 32 && $this->pos < \count($this->array)) {
            $c = $this->array[$this->pos];
            $this->pos++;
            $val = $c & 0x7f;

            if (($val << $shift >> $shift) !== $val) {
                return -1;
            }
            $result |= $val << $shift;

            if ($c < 128) {
                return $result;
            }
            $shift += 7;
        }

        return -1;
    }

    public function uncompressToBuffer(array &$outBuffer) : bool
    {
        $array = $this->array;
        $arrayLength = \count($array);
        $outBuffer = \array_fill(0, $this->readUncompressedLength(), 0);
        $pos = $this->pos;
        $outPos = 0;
        $len = $offset = 0;

        while ($pos < \count($array)) {
            $c = $array[$pos];
            $pos++;

            if (($c & 0x3) === 0) {
                // Literal
                $len = ($c >> 2) + 1;

                if ($len > 60) {
                    if ($pos + 3 >= $arrayLength) {
                        return false;
                    }
                    $smallLen = $len - 60;
                    $len = $array[$pos] + ($array[$pos + 1] << 8) + ($array[$pos + 2] << 16) + ($array[$pos + 3] << 24);
                    $len = ($len & self::WORD_MASK[$smallLen]) + 1;
                    $pos += $smallLen;
                }

                if ($pos + $len > $arrayLength) {
                    return false;
                }
                $this->copyBytes($array, $pos, $outBuffer, $outPos, $len);
                $pos += $len;
                $outPos += $len;
            } else {
                switch ($c & 0x3) {
                    case 1:
                        $len = (($c >> 2) & 0x7) + 4;
                        $offset = $array[$pos] + (($c >> 5) << 8);
                        $pos += 1;

                        break;
                    case 2:
                        if ($pos + 1 >= $arrayLength) {
                            return false;
                        }
                        $len = ($c >> 2) + 1;
                        $offset = $array[$pos] + ($array[$pos + 1] << 8);
                        $pos += 2;

                        break;
                    case 3:
                        if ($pos + 3 >= $arrayLength) {
                            return false;
                        }
                        $len = ($c >> 2) + 1;
                        $offset = $array[$pos] + ($array[$pos + 1] << 8) + ($array[$pos + 2] << 16) + ($array[$pos + 3] << 24);
                        $pos += 4;

                        break;
                }

                if ($offset === 0 || $offset > $outPos) {
                    return false;
                }
                $this->selfCopyBytes($outBuffer, $outPos, $offset, $len);
                $outPos += $len;
            }
        }

        return true;
    }

    private function copyBytes(array $fromArray, int $fromPos, array &$toArray, int $toPos, int $length) : void
    {
        for ($i = 0; $i < $length; $i++) {
            $toArray[$toPos + $i] = $fromArray[$fromPos + $i];
        }
    }

    private function selfCopyBytes(array &$array, int $pos, int $offset, int $length) : void
    {
        for ($i = 0; $i < $length; $i++) {
            $array[$pos + $i] = $array[$pos - $offset + $i];
        }
    }
}
