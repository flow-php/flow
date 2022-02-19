<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Factory;

use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Transformer\Cast\CastToDateTime;
use Flow\ETL\Transformer\Factory\ArrayRowsFactory;
use Flow\ETL\Transformer\Factory\CastedRowsFactory;
use PHPUnit\Framework\TestCase;

final class CasedRowsFactoryTest extends TestCase
{
    public function test_creating_casted_rows_nullable() : void
    {
        $rows = (new CastedRowsFactory(
            new ArrayRowsFactory(),
            CastToDateTime::nullable(['blocked_at'], 'Y-m-d H:i:s', 'UTC')
        ))->create(
            [
                ['id' => 1, 'name' => 'Norbert', 'roles' => ['USER', 'ADMIN'], 'blocked_at' => null],
            ]
        );

        $this->assertInstanceOf(NullEntry::class, $rows->first()->get('blocked_at'));
    }

    public function test_creating_casted_rows_not_nullable() : void
    {
        $rows = (new CastedRowsFactory(
            new ArrayRowsFactory(),
            new CastToDateTime(['blocked_at'], 'UTC')
        ))->create(
            [
                ['id' => 2, 'name' => 'John', 'roles' => ['USER'], 'blocked_at' => '2020-01-01 00:00:00'],
            ]
        );

        $this->assertInstanceOf(DateTimeEntry::class, $rows->first()->get('blocked_at'));
    }
}
