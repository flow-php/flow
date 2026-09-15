<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Doctrine\DBAL\Exception\TableNotFoundException;
use Flow\ETL\Adapter\Doctrine\ProbeRefusal;
use Flow\ETL\Adapter\Doctrine\Tests\Context\InMemorySqlite;
use Flow\ETL\Tests\FlowTestCase;
use RuntimeException;

final class ProbeRefusalTest extends FlowTestCase
{
    public function test_dbal_converts_a_missing_sqlite_table_to_table_not_found(): void
    {
        static::assertInstanceOf(TableNotFoundException::class, InMemorySqlite::connection()
            ->getDriver()
            ->getExceptionConverter()
            ->convert(new ProbeRefusal('no such table: missing'), null));
    }

    public function test_it_carries_message_code_sqlstate_and_previous(): void
    {
        $previous = new RuntimeException('native');
        $refusal = new ProbeRefusal('refused', 1146, '42S02', $previous);

        static::assertSame('refused', $refusal->getMessage());
        static::assertSame(1146, $refusal->getCode());
        static::assertSame('42S02', $refusal->getSQLState());
        static::assertSame($previous, $refusal->getPrevious());
    }
}
