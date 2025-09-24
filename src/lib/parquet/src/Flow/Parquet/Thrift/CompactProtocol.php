<?php

declare(strict_types=1);

namespace Flow\Parquet\Thrift;

use Thrift\Exception\TProtocolException;
use Thrift\Factory\TStringFuncFactory;
use Thrift\Protocol\TProtocol;
use Thrift\Type\TType;

class CompactProtocol extends TProtocol
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

    protected static $ctypes = [
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

    protected static $ttypes = [
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

    protected $boolFid;

    protected $boolValue;

    protected $containers = [];

    protected $lastFid = 0;

    protected $state = self::STATE_CLEAR;

    protected $structs = [];

    public function __construct($trans)
    {
        parent::__construct($trans);
    }

    public function fromZigZag($n)
    {
        return ($n >> 1) ^ -($n & 1);
    }

    public function getTType($byte)
    {
        return self::$ttypes[$byte & 0x0F];
    }

    public function getVarint($data)
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

    public function readBool(&$value)
    {
        if ($this->state === self::STATE_BOOL_READ) {
            $value = $this->boolValue;

            return 0;
        }

        if ($this->state === self::STATE_CONTAINER_READ) {
            return $this->readByte($value);
        }

        throw new TProtocolException('Invalid state in compact protocol');

    }

    public function readByte(&$value)
    {
        $data = $this->trans_->readAll(1);
        $byte = ord($data);
        $value = $byte > 127 ? $byte - 256 : $byte;

        return 1;
    }

    public function readCollectionBegin(&$type, &$size)
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

    public function readCollectionEnd()
    {
        $this->state = array_pop($this->containers);

        return 0;
    }

    public function readDouble(&$value)
    {
        $data = $this->trans_->readAll(8);
        $arr = unpack('d', $data);
        $value = $arr[1];

        return 8;
    }

    public function readFieldBegin(&$name, &$field_type, &$field_id)
    {
        $result = $this->readUByte($compact_type_and_delta);

        $compact_type = $compact_type_and_delta & 0x0F;

        if ($compact_type === TType::STOP) {
            $field_type = $compact_type;
            $field_id = 0;

            return $result;
        }
        $delta = $compact_type_and_delta >> 4;

        if ($delta === 0) {
            $result += $this->readI16($field_id);
        } else {
            $field_id = $this->lastFid + $delta;
        }
        $this->lastFid = $field_id;
        $field_type = $this->getTType($compact_type);

        if ($compact_type === self::COMPACT_TRUE) {
            $this->state = self::STATE_BOOL_READ;
            $this->boolValue = true;
        } elseif ($compact_type === self::COMPACT_FALSE) {
            $this->state = self::STATE_BOOL_READ;
            $this->boolValue = false;
        } else {
            $this->state = self::STATE_VALUE_READ;
        }

        return $result;
    }

    public function readFieldEnd()
    {
        $this->state = self::STATE_FIELD_READ;

        return 0;
    }

    public function readI16(&$value)
    {
        return $this->readZigZag($value);
    }

    public function readI32(&$value)
    {
        return $this->readZigZag($value);
    }

    // If we are on a 32bit architecture we have to explicitly deal with
    // 64-bit twos-complement arithmetic since PHP wants to treat all ints
    // as signed and any int over 2^31 - 1 as a float

    // Read and write I64 as two 32 bit numbers $hi and $lo

    public function readI64(&$value)
    {
        // Read varint from wire
        $hi = 0;
        $lo = 0;

        $idx = 0;
        $shift = 0;

        while (true) {
            $x = $this->trans_->readAll(1);
            $byte = ord($x);
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
        $value = -$hi * 4294967296 - $lo;

        if (!$isNeg) {
            $value = -$value;
        }

        return $idx;
    }

    public function readListBegin(&$elem_type, &$size)
    {
        return $this->readCollectionBegin($elem_type, $size);
    }

    public function readListEnd()
    {
        return $this->readCollectionEnd();
    }

    public function readMapBegin(&$key_type, &$val_type, &$size)
    {
        $result = $this->readVarint($size);
        $types = 0;

        if ($size > 0) {
            $result += $this->readUByte($types);
        }
        $val_type = $this->getTType($types);
        $key_type = $this->getTType($types >> 4);
        $this->containers[] = $this->state;
        $this->state = self::STATE_CONTAINER_READ;

        return $result;
    }

    public function readMapEnd()
    {
        return $this->readCollectionEnd();
    }

    public function readMessageBegin(&$name, &$type, &$seqid)
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

    public function readMessageEnd()
    {
        return 0;
    }

    public function readSetBegin(&$elem_type, &$size)
    {
        return $this->readCollectionBegin($elem_type, $size);
    }

    public function readSetEnd()
    {
        return $this->readCollectionEnd();
    }

    public function readString(&$value)
    {
        $result = $this->readVarint($len);

        if ($len) {
            $value = $this->trans_->readAll($len);
        } else {
            $value = '';
        }

        return $result + $len;
    }

    public function readStructBegin(&$name)
    {
        $name = ''; // unused
        $this->structs[] = [$this->state, $this->lastFid];
        $this->state = self::STATE_FIELD_READ;
        $this->lastFid = 0;

        return 0;
    }

    public function readStructEnd()
    {
        $last = array_pop($this->structs);
        $this->state = $last[0];
        $this->lastFid = $last[1];

        return 0;
    }

    public function readUByte(&$value)
    {
        $data = $this->trans_->readAll(1);
        $value = ord($data);

        return 1;
    }

    public function readVarint(&$result)
    {
        $idx = 0;
        $shift = 0;
        $result = 0;

        while (true) {
            $x = $this->trans_->readAll(1);
            $byte = ord($x);
            $idx++;
            $result |= ($byte & 0x7F) << $shift;

            if (($byte >> 7) === 0) {
                return $idx;
            }
            $shift += 7;
        }
    }

    public function readZigZag(&$value)
    {
        $result = $this->readVarint($value);
        $value = $this->fromZigZag($value);

        return $result;
    }

    // Some varint / zigzag helper methods
    public function toZigZag($n, $bits)
    {
        return ($n << 1) ^ ($n >> ($bits - 1));
    }

    public function writeBool($value)
    {
        if ($this->state == self::STATE_BOOL_WRITE) {
            $ctype = self::COMPACT_FALSE;

            if ($value) {
                $ctype = self::COMPACT_TRUE;
            }

            return $this->writeFieldHeader($ctype, $this->boolFid);
        }

        if ($this->state == self::STATE_CONTAINER_WRITE) {
            return $this->writeByte($value ? 1 : 0);
        }

        throw new TProtocolException('Invalid state in compact protocol');

    }

    public function writeByte($value)
    {
        $data = pack('c', $value);
        $this->trans_->write($data, 1);

        return 1;
    }

    public function writeCollectionBegin($etype, $size)
    {
        $written = 0;

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

    public function writeCollectionEnd()
    {
        $this->state = array_pop($this->containers);

        return 0;
    }

    public function writeDouble($value)
    {
        $data = pack('d', $value);
        $this->trans_->write($data, 8);

        return 8;
    }

    public function writeFieldBegin($field_name, $field_type, $field_id)
    {
        if ($field_type === TTYPE::BOOL) {
            $this->state = self::STATE_BOOL_WRITE;
            $this->boolFid = $field_id;

            return 0;
        }
        $this->state = self::STATE_VALUE_WRITE;

        return $this->writeFieldHeader(self::$ctypes[$field_type], $field_id);

    }

    public function writeFieldEnd()
    {
        $this->state = self::STATE_FIELD_WRITE;

        return 0;
    }

    public function writeFieldHeader($type, $fid)
    {
        $written = 0;
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

    public function writeFieldStop()
    {
        return $this->writeByte(0);
    }

    public function writeI16($value)
    {
        $thing = $this->toZigZag($value, 16);

        return $this->writeVarint($thing);
    }

    public function writeI32($value)
    {
        $thing = $this->toZigZag($value, 32);

        return $this->writeVarint($thing);
    }

    public function writeI64($value)
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
            $lo = ($lo << 1);
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
                $lo = $lo >> 7;
                $lo |= ($hi << 25);
                $hi = $hi >> 7;
                // Right shift carries sign, but we don't want it to.
                $hi &= (127 << 25);

            }

            $ret = TStringFuncFactory::create()->strlen($out);
            $this->trans_->write($out, $ret);

            return $ret;
        }

        return $this->writeVarint($this->toZigZag($value, 64));

    }

    public function writeListBegin($elem_type, $size)
    {
        return $this->writeCollectionBegin($elem_type, $size);
    }

    public function writeListEnd()
    {
        return $this->writeCollectionEnd();
    }

    public function writeMapBegin($key_type, $val_type, $size)
    {
        $written = 0;

        if ($size == 0) {
            $written = $this->writeByte(0);
        } else {
            $written = $this->writeVarint($size) +
                $this->writeUByte(self::$ctypes[$key_type] << 4 |
                    self::$ctypes[$val_type]);
        }
        $this->containers[] = $this->state;

        return $written;
    }

    public function writeMapEnd()
    {
        return $this->writeCollectionEnd();
    }

    public function writeMessageBegin($name, $type, $seqid)
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

    public function writeMessageEnd()
    {
        $this->state = self::STATE_CLEAR;

        return 0;
    }

    public function writeSetBegin($elem_type, $size)
    {
        return $this->writeCollectionBegin($elem_type, $size);
    }

    public function writeSetEnd()
    {
        return $this->writeCollectionEnd();
    }

    public function writeString($value)
    {
        $len = TStringFuncFactory::create()->strlen($value);
        $result = $this->writeVarint($len);

        if ($len) {
            $this->trans_->write($value, $len);
        }

        return $result + $len;
    }

    public function writeStructBegin($name)
    {
        $this->structs[] = [$this->state, $this->lastFid];
        $this->state = self::STATE_FIELD_WRITE;
        $this->lastFid = 0;

        return 0;
    }

    public function writeStructEnd()
    {
        $old_values = array_pop($this->structs);
        $this->state = $old_values[0];
        $this->lastFid = $old_values[1];

        return 0;
    }

    public function writeUByte($byte)
    {
        $this->trans_->write(pack('C', $byte), 1);

        return 1;
    }

    public function writeVarint($data)
    {
        $out = $this->getVarint($data);
        $result = TStringFuncFactory::create()->strlen($out);
        $this->trans_->write($out, $result);

        return $result;
    }
}
