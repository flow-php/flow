<?php declare(strict_types=1);
namespace Flow\Parquet\Thrift;

/**
 * Autogenerated by Thrift Compiler (0.19.0).
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *
 *  @generated
 */
use Thrift\Base\TBase;
use Thrift\Type\TType;

/**
 * Union to specify the order used for the min_value and max_value fields for a
 * column. This union takes the role of an enhanced enum that allows rich
 * elements (which will be needed for a collation-based ordering in the future).
 *
 * Possible values are:
 * * TypeDefinedOrder - the column uses the order defined by its logical or
 *                      physical type (if there is no logical type).
 *
 * If the reader does not support the value of this union, min and max stats
 * for this column should be ignored.
 */
class ColumnOrder extends TBase
{
    public static $_TSPEC = [
        1 => [
            'var' => 'TYPE_ORDER',
            'isRequired' => false,
            'type' => TType::STRUCT,
            'class' => '\Flow\Parquet\Thrift\TypeDefinedOrder',
        ],
    ];

    public static $isValidate = false;

    /**
     * The sort orders for logical types are:
     *   UTF8 - unsigned byte-wise comparison
     *   INT8 - signed comparison
     *   INT16 - signed comparison
     *   INT32 - signed comparison
     *   INT64 - signed comparison
     *   UINT8 - unsigned comparison
     *   UINT16 - unsigned comparison
     *   UINT32 - unsigned comparison
     *   UINT64 - unsigned comparison
     *   DECIMAL - signed comparison of the represented value
     *   DATE - signed comparison
     *   TIME_MILLIS - signed comparison
     *   TIME_MICROS - signed comparison
     *   TIMESTAMP_MILLIS - signed comparison
     *   TIMESTAMP_MICROS - signed comparison
     *   INTERVAL - unsigned comparison
     *   JSON - unsigned byte-wise comparison
     *   BSON - unsigned byte-wise comparison
     *   ENUM - unsigned byte-wise comparison
     *   LIST - undefined
     *   MAP - undefined.
     *
     * In the absence of logical types, the sort order is determined by the physical type:
     *   BOOLEAN - false, true
     *   INT32 - signed comparison
     *   INT64 - signed comparison
     *   INT96 (only used for legacy timestamps) - undefined
     *   FLOAT - signed comparison of the represented value (*)
     *   DOUBLE - signed comparison of the represented value (*)
     *   BYTE_ARRAY - unsigned byte-wise comparison
     *   FIXED_LEN_BYTE_ARRAY - unsigned byte-wise comparison
     *
     * (*) Because the sorting order is not specified properly for floating
     *     point values (relations vs. total ordering) the following
     *     compatibility rules should be applied when reading statistics:
     *     - If the min is a NaN, it should be ignored.
     *     - If the max is a NaN, it should be ignored.
     *     - If the min is +0, the row group may contain -0 values as well.
     *     - If the max is -0, the row group may contain +0 values as well.
     *     - When looking for NaN values, min and max should be ignored.
     *
     *     When writing statistics the following rules should be followed:
     *     - NaNs should not be written to min or max statistics fields.
     *     - If the computed max value is zero (whether negative or positive),
     *       `+0.0` should be written into the max statistics field.
     *     - If the computed min value is zero (whether negative or positive),
     *       `-0.0` should be written into the min statistics field.
     *
     * @var TypeDefinedOrder
     */
    public $TYPE_ORDER;

    public function __construct($vals = null)
    {
        if (\is_array($vals)) {
            parent::__construct(self::$_TSPEC, $vals);
        }
    }

    public function getName()
    {
        return 'ColumnOrder';
    }

    public function read($input)
    {
        return $this->_read('ColumnOrder', self::$_TSPEC, $input);
    }

    public function write($output)
    {
        return $this->_write('ColumnOrder', self::$_TSPEC, $output);
    }
}
