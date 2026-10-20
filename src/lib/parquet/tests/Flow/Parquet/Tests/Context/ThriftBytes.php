<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use Flow\Parquet\Thrift\CompactProtocol;
use Flow\Parquet\Thrift\MemoryBuffer;
use Flow\Parquet\ThriftModel\BloomFilterHeader;
use Flow\Parquet\ThriftModel\ColumnChunk;
use Flow\Parquet\ThriftModel\ColumnIndex;
use Flow\Parquet\ThriftModel\ColumnOrder;
use Flow\Parquet\ThriftModel\OffsetIndex;
use Flow\Parquet\ThriftModel\PageHeader;

final class ThriftBytes
{
    /**
     * The compact-protocol bytes of a thrift struct, as a file holds them.
     */
    public static function of(BloomFilterHeader|ColumnChunk|ColumnIndex|ColumnOrder|OffsetIndex|PageHeader $struct): string
    {
        $struct->write(new CompactProtocol($buffer = new MemoryBuffer()));

        return $buffer->data();
    }
}
