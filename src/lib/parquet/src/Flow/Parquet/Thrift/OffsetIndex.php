<?php

declare(strict_types=1);
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

class OffsetIndex extends TBase
{
    public static $_TSPEC = [
        1 => [
            'var' => 'page_locations',
            'isRequired' => true,
            'type' => TType::LST,
            'etype' => TType::STRUCT,
            'elem' => [
                'type' => TType::STRUCT,
                'class' => '\Flow\Parquet\Thrift\PageLocation',
            ],
        ],
    ];

    public static $isValidate = false;

    /**
     * PageLocations, ordered by increasing PageLocation.offset. It is required
     * that page_locations[i].first_row_index < page_locations[i+1].first_row_index.
     *
     * @var PageLocation[]
     */
    public $page_locations;

    public function __construct($vals = null)
    {
        if (\is_array($vals)) {
            parent::__construct(self::$_TSPEC, $vals);
        }
    }

    public function getName()
    {
        return 'OffsetIndex';
    }

    public function read($input)
    {
        return $this->_read('OffsetIndex', self::$_TSPEC, $input);
    }

    public function write($output)
    {
        return $this->_write('OffsetIndex', self::$_TSPEC, $output);
    }
}
