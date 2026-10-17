<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use Flow\Parquet\Thrift\CompactProtocol;
use Flow\Parquet\Thrift\MemoryBuffer;
use Flow\Parquet\ThriftModel\ColumnChunk;
use Flow\Parquet\ThriftModel\ColumnOrder;

final class ThriftBytes
{
    /**
     * The compact-protocol bytes of a thrift struct, as a footer holds them.
     */
    public static function of(ColumnChunk|ColumnOrder $struct): string
    {
        $struct->write(new CompactProtocol($buffer = new MemoryBuffer()));

        return $buffer->data();
    }
}
