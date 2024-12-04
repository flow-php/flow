<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Schema;

use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, Repetition};
use PHPUnit\Framework\TestCase;

final class FlatColumnTest extends TestCase
{
    public function test_is_map_on_a_non_map_column() : void
    {
        self::assertFalse(FlatColumn::int32('int32')->isMap());
    }

    public function test_repetitions() : void
    {
        self::assertSame([Repetition::OPTIONAL], Schema::with(FlatColumn::int32('int32'))->get('int32')->repetitions()->toArray());
    }
}
