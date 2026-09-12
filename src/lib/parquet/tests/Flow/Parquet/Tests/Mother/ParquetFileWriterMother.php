<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Mother;

use Flow\Filesystem\DestinationStream;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFileWriter;

final class ParquetFileWriterMother
{
    public static function open(
        ParquetEngine $engine,
        DestinationStream $stream,
        Options $options = new Options(),
    ): ParquetFileWriter {
        return $engine->openForWrite(
            $stream,
            Schema::with(FlatColumn::int32('id')),
            Compressions::UNCOMPRESSED,
            $options,
        );
    }
}
