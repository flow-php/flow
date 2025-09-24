<?php

declare(strict_types=1);

namespace Flow\Parquet\Thrift;

use Thrift\Exception\{TProtocolException, TTransportException};
use Thrift\Type\TType;

class CompactProtocol
{
    public const COMPACT_BINARY = 0x08;

    public const COMPACT_BYTE = 0x03;

    public const COMPACT_DOUBLE = 0x07;

    public const COMPACT_FALSE = 0x02;

    public const COMPACT_I16 = 0x04;

    public const COMPACT_I32 = 0x05;

    public const COMPACT_I64 = 0x06;

    public const COMPACT_LIST = 0x09;

    public const COMPACT_MAP = 0x0B;

    public const COMPACT_SET = 0x0A;

    public const COMPACT_STOP = 0x00;

    public const COMPACT_STRUCT = 0x0C;

    public const COMPACT_TRUE = 0x01;

    public const PROTOCOL_ID = 0x82;

    public const STATE_BOOL_READ = 8;

    public const STATE_BOOL_WRITE = 4;

    public const STATE_CLEAR = 0;

    public const STATE_CONTAINER_READ = 6;

    public const STATE_CONTAINER_WRITE = 3;

    public const STATE_FIELD_READ = 5;

    public const STATE_FIELD_WRITE = 1;

    public const STATE_VALUE_READ = 7;

    public const STATE_VALUE_WRITE = 2;

    public const TYPE_BITS = 0x07;

    public const TYPE_MASK = 0xE0;

    public const TYPE_SHIFT_AMOUNT = 5;

    public const VERSION = 1;

    public const VERSION_MASK = 0x1F;

    /**
     * @var array<int, int>
     */
    protected static array $ctypes = [
        TType::STOP => self::COMPACT_STOP,
        TType::BOOL => self::COMPACT_TRUE, // used for collection
        TType::BYTE => self::COMPACT_BYTE,
        TType::I16 => self::COMPACT_I16,
        TType::I32 => self::COMPACT_I32,
        TType::I64 => self::COMPACT_I64,
        TType::DOUBLE => self::COMPACT_DOUBLE,
        TType::STRING => self::COMPACT_BINARY,
        TType::STRUCT => self::COMPACT_STRUCT,
        TType::LST => self::COMPACT_LIST,
        TType::SET => self::COMPACT_SET,
        TType::MAP => self::COMPACT_MAP,
    ];

    /**
     * @var array<int, int>
     */
    protected static array $ttypes = [
        self::COMPACT_STOP => TType::STOP,
        self::COMPACT_TRUE => TType::BOOL, // used for collection
        self::COMPACT_FALSE => TType::BOOL,
        self::COMPACT_BYTE => TType::BYTE,
        self::COMPACT_I16 => TType::I16,
        self::COMPACT_I32 => TType::I32,
        self::COMPACT_I64 => TType::I64,
        self::COMPACT_DOUBLE => TType::DOUBLE,
        self::COMPACT_BINARY => TType::STRING,
        self::COMPACT_STRUCT => TType::STRUCT,
        self::COMPACT_LIST => TType::LST,
        self::COMPACT_SET => TType::SET,
        self::COMPACT_MAP => TType::MAP,
    ];

    protected int $boolFid;

    protected bool $boolValue;

    protected array $containers = [];

    protected int $lastFid = 0;

    protected int $state = self::STATE_CLEAR;

    protected array $structs = [];

    public function __construct(private readonly Transport $transport)
    {
    }

    public function fromZigZag(int $n) : int
    {
        return ($n >> 1) ^ -($n & 1);
    }

    public function getTransport() : Transport
    {
        return $this->transport;
    }

    public function getTType(int $byte) : int
    {
        return self::$ttypes[$byte & 0x0F];
    }

    public function getVarint(int $data) : string
    {
        $out = '';

        while (true) {
            if (($data & ~0x7F) === 0) {
                $out .= chr($data);

                break;
            }
            $out .= chr(($data & 0xFF) | 0x80);
            $data >>= 7;

        }

        return $out;
    }

    public function readBool(&$bool) : int
    {
        if ($this->state === self::STATE_BOOL_READ) {
            $bool = $this->boolValue;

            return 0;
        }

        if ($this->state === self::STATE_CONTAINER_READ) {
            return $this->readByte($bool);
        }

        throw new TProtocolException('Invalid state in compact protocol');

    }

    public function readByte(&$byte) : int
    {
        $data = $this->transport->read(1);
        $readByte = ord($data[0]);
        $byte = $readByte > 127 ? $readByte - 256 : $readByte;

        return 1;
    }

    public function readCollectionBegin(&$type, &$size) : int
    {
        $sizeType = 0;
        $result = $this->readUByte($sizeType);
        $size = $sizeType >> 4;
        $type = $this->getTType($sizeType);

        if ($size === 15) {
            $result += $this->readVarint($size);
        }
        $this->containers[] = $this->state;
        $this->state = self::STATE_CONTAINER_READ;

        return $result;
    }

    public function readCollectionEnd() : int
    {
        $this->state = array_pop($this->containers);

        return 0;
    }

    public function readDouble(&$dub) : int
    {
        $data = $this->transport->read(8);
        $arr = unpack('d', $data);
        $dub = $arr[1];

        return 8;
    }

    public function readFieldBegin(&$name, &$fieldType, &$fieldId) : int
    {
        $result = $this->readUByte($compactTypeAndDelta);

        $compactType = $compactTypeAndDelta & 0x0F;

        if ($compactType === TType::STOP) {
            $fieldType = $compactType;
            $fieldId = 0;

            return $result;
        }
        $delta = $compactTypeAndDelta >> 4;

        if ($delta === 0) {
            $result += $this->readI16($fieldId);
        } else {
            $fieldId = $this->lastFid + $delta;
        }
        $this->lastFid = $fieldId;
        $fieldType = $this->getTType($compactType);

        if ($compactType === self::COMPACT_TRUE) {
            $this->state = self::STATE_BOOL_READ;
            $this->boolValue = true;
        } elseif ($compactType === self::COMPACT_FALSE) {
            $this->state = self::STATE_BOOL_READ;
            $this->boolValue = false;
        } else {
            $this->state = self::STATE_VALUE_READ;
        }

        return $result;
    }

    public function readFieldEnd() : int
    {
        $this->state = self::STATE_FIELD_READ;

        return 0;
    }

    public function readI16(&$i16) : int
    {
        return $this->readZigZag($i16);
    }

    public function readI32(&$i32) : int
    {
        return $this->readZigZag($i32);
    }

    /**
     * If we are on a 32bit architecture we have to explicitly deal with
     * 64-bit twos-complement arithmetic since PHP wants to treat all ints
     * as signed and any int over 2^31 - 1 as a float.
     *
     * Read and write I64 as two 32 bit numbers $hi and $lo
     *
     * @throws TTransportException
     */
    public function readI64(&$i64) : int
    {
        // Read varint from wire
        $hi = 0;
        $lo = 0;

        $idx = 0;
        $shift = 0;

        while (true) {
            $x = $this->transport->read(1);
            $byte = ord($x[0]);
            $idx++;

            // Shift hi and lo together.
            if ($shift < 28) {
                $lo |= (($byte & 0x7F) << $shift);
            } elseif ($shift === 28) {
                $lo |= (($byte & 0x0F) << 28);
                $hi |= (($byte & 0x70) >> 4);
            } else {
                $hi |= (($byte & 0x7F) << ($shift - 32));
            }

            if (($byte >> 7) === 0) {
                break;
            }
            $shift += 7;
        }

        // Now, unzig it.
        $xorer = 0;

        if ($lo & 1) {
            $xorer = 0xFFFFFFFF;
        }
        $lo = ($lo >> 1) & 0x7FFFFFFF;
        $lo |= (($hi & 1) << 31);
        $hi = ($hi >> 1) ^ $xorer;
        $lo ^= $xorer;

        // Now put $hi and $lo back together
        $isNeg = $hi < 0 || $hi & 0x80000000;

        // Check for a negative
        if ($isNeg) {
            $hi = ~$hi & (int) 0xFFFFFFFF;
            $lo = ~$lo & (int) 0xFFFFFFFF;

            if ($lo === (int) 0xFFFFFFFF) {
                $hi++;
                $lo = 0;
            } else {
                $lo++;
            }
        }

        // Force 32bit words in excess of 2G to be positive - we deal with sign
        // explicitly below
        if ($hi & (int) 0x80000000) {
            $hi &= (int) 0x7FFFFFFF;
            $hi += 0x80000000;
        }

        if ($lo & (int) 0x80000000) {
            $lo &= (int) 0x7FFFFFFF;
            $lo += 0x80000000;
        }

        // Create as negative value first, since we can store -2^63 but not 2^63
        $i64 = -$hi * 4294967296 - $lo;

        if (!$isNeg) {
            $i64 = -$i64;
        }

        return $idx;
    }

    public function readListBegin(&$elemType, &$size) : int
    {
        return $this->readCollectionBegin($elemType, $size);
    }

    public function readListEnd() : int
    {
        return $this->readCollectionEnd();
    }

    public function readMapBegin(&$keyType, &$valType, &$size) : int
    {
        $result = $this->readVarint($size);
        $types = 0;

        if ($size > 0) {
            $result += $this->readUByte($types);
        }
        $valType = $this->getTType($types);
        $keyType = $this->getTType($types >> 4);
        $this->containers[] = $this->state;
        $this->state = self::STATE_CONTAINER_READ;

        return $result;
    }

    public function readMapEnd() : int
    {
        return $this->readCollectionEnd();
    }

    public function readMessageBegin(&$name, &$type, &$seqid) : int
    {
        $protoId = 0;
        $result = $this->readUByte($protoId);

        if ($protoId !== self::PROTOCOL_ID) {
            throw new TProtocolException('Bad protocol id in TCompact message');
        }
        $verType = 0;
        $result += $this->readUByte($verType);
        $type = ($verType >> self::TYPE_SHIFT_AMOUNT) & self::TYPE_BITS;
        $version = $verType & self::VERSION_MASK;

        if ($version !== self::VERSION) {
            throw new TProtocolException('Bad version in TCompact message');
        }
        $result += $this->readVarint($seqid);
        $result += $this->readString($name);

        return $result;
    }

    public function readMessageEnd() : int
    {
        return 0;
    }

    public function readSetBegin(&$elemType, &$size) : int
    {
        return $this->readCollectionBegin($elemType, $size);
    }

    public function readSetEnd() : int
    {
        return $this->readCollectionEnd();
    }

    public function readString(&$str) : int
    {
        $result = $this->readVarint($len);

        if ($len) {
            $str = $this->transport->read($len);
        } else {
            $str = '';
        }

        return $result + $len;
    }

    public function readStructBegin(&$name) : int
    {
        $name = ''; // unused
        $this->structs[] = [$this->state, $this->lastFid];
        $this->state = self::STATE_FIELD_READ;
        $this->lastFid = 0;

        return 0;
    }

    public function readStructEnd() : int
    {
        $last = array_pop($this->structs);
        $this->state = $last[0];
        $this->lastFid = $last[1];

        return 0;
    }

    public function readUByte(&$value) : int
    {
        $data = $this->transport->read(1);
        $value = ord($data[0]);

        return 1;
    }

    public function readVarint(&$result) : int
    {
        $idx = 0;
        $shift = 0;
        $result = 0;

        while (true) {
            $x = $this->transport->read(1);
            $byte = ord($x[0]);
            $idx++;
            $result |= ($byte & 0x7F) << $shift;

            if (($byte >> 7) === 0) {
                return $idx;
            }
            $shift += 7;
        }
    }

    public function readZigZag(&$value) : int
    {
        $result = $this->readVarint($value);
        $value = $this->fromZigZag($value);

        return $result;
    }

    public function skip($type)
    {
        switch ($type) {
            case TType::BOOL:
                return $this->readBool($bool);
            case TType::BYTE:
                return $this->readByte($byte);
            case TType::I16:
                return $this->readI16($i16);
            case TType::I32:
                return $this->readI32($i32);
            case TType::I64:
                return $this->readI64($i64);
            case TType::DOUBLE:
                return $this->readDouble($dub);
            case TType::STRING:
                return $this->readString($str);
            case TType::STRUCT:
                $result = $this->readStructBegin($name);

                while (true) {
                    $result += $this->readFieldBegin($name, $ftype, $fid);

                    if ($ftype == TType::STOP) {
                        break;
                    }
                    $result += $this->skip($ftype);
                    $result += $this->readFieldEnd();
                }
                $result += $this->readStructEnd();

                return $result;

            case TType::MAP:
                $result = $this->readMapBegin($keyType, $valType, $size);

                for ($i = 0; $i < $size; $i++) {
                    $result += $this->skip($keyType);
                    $result += $this->skip($valType);
                }
                $result += $this->readMapEnd();

                return $result;

            case TType::SET:
                $result = $this->readSetBegin($elemType, $size);

                for ($i = 0; $i < $size; $i++) {
                    $result += $this->skip($elemType);
                }
                $result += $this->readSetEnd();

                return $result;

            case TType::LST:
                $result = $this->readListBegin($elemType, $size);

                for ($i = 0; $i < $size; $i++) {
                    $result += $this->skip($elemType);
                }
                $result += $this->readListEnd();

                return $result;

            default:
                throw new TProtocolException(
                    'Unknown field type: ' . $type,
                    TProtocolException::INVALID_DATA
                );
        }
    }

    public function toZigZag($n, $bits) : int
    {
        return ($n << 1) ^ ($n >> ($bits - 1));
    }

    public function writeBool($bool) : int
    {
        if ($this->state === self::STATE_BOOL_WRITE) {
            $ctype = self::COMPACT_FALSE;

            if ($bool) {
                $ctype = self::COMPACT_TRUE;
            }

            return $this->writeFieldHeader($ctype, $this->boolFid);
        }

        if ($this->state === self::STATE_CONTAINER_WRITE) {
            return $this->writeByte($bool ? 1 : 0);
        }

        throw new TProtocolException('Invalid state in compact protocol');

    }

    public function writeByte(int $byte) : int
    {
        $data = pack('c', $byte);
        $this->transport->write($data, 1);

        return 1;
    }

    public function writeCollectionBegin(int $etype, int $size) : int
    {
        if ($size <= 14) {
            $written = $this->writeUByte($size << 4 |
                self::$ctypes[$etype]);
        } else {
            $written = $this->writeUByte(0xF0 |
                    self::$ctypes[$etype]) +
                $this->writeVarint($size);
        }
        $this->containers[] = $this->state;
        $this->state = self::STATE_CONTAINER_WRITE;

        return $written;
    }

    public function writeCollectionEnd() : int
    {
        $this->state = array_pop($this->containers);

        return 0;
    }

    public function writeDouble(float $dub) : int
    {
        $data = pack('d', $dub);
        $this->transport->write($data, 8);

        return 8;
    }

    public function writeFieldBegin(string $fieldName, int $fieldType, int $fieldId) : int
    {
        if ($fieldType === TTYPE::BOOL) {
            $this->state = self::STATE_BOOL_WRITE;
            $this->boolFid = $fieldId;

            return 0;
        }
        $this->state = self::STATE_VALUE_WRITE;

        return $this->writeFieldHeader(self::$ctypes[$fieldType], $fieldId);

    }

    public function writeFieldEnd() : int
    {
        $this->state = self::STATE_FIELD_WRITE;

        return 0;
    }

    public function writeFieldHeader(int $type, int $fid) : int
    {
        $delta = $fid - $this->lastFid;

        if (0 < $delta && $delta <= 15) {
            $written = $this->writeUByte(($delta << 4) | $type);
        } else {
            $written = $this->writeByte($type) +
                $this->writeI16($fid);
        }
        $this->lastFid = $fid;

        return $written;
    }

    public function writeFieldStop() : int
    {
        return $this->writeByte(0);
    }

    public function writeI16(int $value) : int
    {
        $thing = $this->toZigZag($value, 16);

        return $this->writeVarint($thing);
    }

    public function writeI32(int $value) : int
    {
        $thing = $this->toZigZag($value, 32);

        return $this->writeVarint($thing);
    }

    public function writeI64(int $value) : int
    {
        // If we are in an I32 range, use the easy method below.
        if (($value > 4294967296) || ($value < -4294967296)) {
            // Convert $value to $hi and $lo
            $neg = $value < 0;

            if ($neg) {
                $value *= -1;
            }

            $hi = (int) $value >> 32;
            $lo = (int) $value & 0xFFFFFFFF;

            if ($neg) {
                $hi = ~$hi;
                $lo = ~$lo;

                if (($lo & (int) 0xFFFFFFFF) === (int) 0xFFFFFFFF) {
                    $lo = 0;
                    $hi++;
                } else {
                    $lo++;
                }
            }

            // Now do the zigging and zagging.
            $xorer = 0;

            if ($neg) {
                $xorer = 0xFFFFFFFF;
            }
            $lowbit = ($lo >> 31) & 1;
            $hi = ($hi << 1) | $lowbit;
            $lo <<= 1;
            $lo = ($lo ^ $xorer) & 0xFFFFFFFF;
            $hi = ($hi ^ $xorer) & 0xFFFFFFFF;

            // now write out the varint, ensuring we shift both hi and lo
            $out = '';

            while (true) {
                if (($lo & ~0x7F) === 0
                    && $hi === 0) {
                    $out .= chr($lo);

                    break;
                }
                $out .= chr(($lo & 0xFF) | 0x80);
                $lo >>= 7;
                $lo |= ($hi << 25);
                $hi >>= 7;
                // Right shift carries sign, but we don't want it to.
                $hi &= (127 << 25);

            }

            $ret = \strlen($out);
            $this->transport->write($out);

            return $ret;
        }

        return $this->writeVarint($this->toZigZag($value, 64));

    }

    public function writeListBegin($elemType, $size) : int
    {
        return $this->writeCollectionBegin($elemType, $size);
    }

    public function writeListEnd() : int
    {
        return $this->writeCollectionEnd();
    }

    public function writeMapBegin(int $keyType, int $valType, int $size) : int
    {
        if ($size === 0) {
            $written = $this->writeByte(0);
        } else {
            $written = $this->writeVarint($size) + $this->writeUByte(self::$ctypes[$keyType] << 4 | self::$ctypes[$valType]);
        }
        $this->containers[] = $this->state;

        return $written;
    }

    public function writeMapEnd() : int
    {
        return $this->writeCollectionEnd();
    }

    public function writeMessageBegin(string $name, int $type, int $seqid) : int
    {
        $written =
            $this->writeUByte(self::PROTOCOL_ID) +
            $this->writeUByte(self::VERSION |
                ($type << self::TYPE_SHIFT_AMOUNT)) +
            $this->writeVarint($seqid) +
            $this->writeString($name);
        $this->state = self::STATE_VALUE_WRITE;

        return $written;
    }

    public function writeMessageEnd() : int
    {
        $this->state = self::STATE_CLEAR;

        return 0;
    }

    public function writeSetBegin(int $elemType, int $size) : int
    {
        return $this->writeCollectionBegin($elemType, $size);
    }

    public function writeSetEnd() : int
    {
        return $this->writeCollectionEnd();
    }

    public function writeString(string $value) : int
    {
        $len = \strlen($value);
        $result = $this->writeVarint($len);

        if ($len) {
            $this->transport->write($value);
        }

        return $result + $len;
    }

    public function writeStructBegin() : int
    {
        $this->structs[] = [$this->state, $this->lastFid];
        $this->state = self::STATE_FIELD_WRITE;
        $this->lastFid = 0;

        return 0;
    }

    public function writeStructEnd() : int
    {
        $oldValues = array_pop($this->structs);
        $this->state = $oldValues[0];
        $this->lastFid = $oldValues[1];

        return 0;
    }

    public function writeUByte(int $byte) : int
    {
        $this->transport->write(pack('C', $byte), 1);

        return 1;
    }

    public function writeVarint(int $data) : int
    {
        $out = $this->getVarint($data);
        $result = \strlen($out);
        $this->transport->write($out);

        return $result;
    }
}
