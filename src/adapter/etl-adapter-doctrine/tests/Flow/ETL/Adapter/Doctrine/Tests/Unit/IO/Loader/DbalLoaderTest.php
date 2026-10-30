<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit\IO\Loader;

use Flow\ETL\Adapter\Doctrine\IO\Loader\DbalLoader;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class DbalLoaderTest extends TestCase
{
    /**
     * @param list<string> $expected
     */
    #[TestWith(['pdo_pgsql', ['pdo', 'pdo_pgsql']])]
    #[TestWith(['pdo_mysql', ['pdo', 'pdo_mysql']])]
    #[TestWith(['pdo_sqlite', ['pdo', 'pdo_sqlite']])]
    #[TestWith(['pdo_sqlsrv', ['pdo', 'pdo_sqlsrv']])]
    #[TestWith(['pdo_oci', ['pdo', 'pdo_oci']])]
    #[TestWith(['mysqli', ['mysqli', 'mysqlnd']])]
    #[TestWith(['pgsql', ['pgsql']])]
    #[TestWith(['sqlite3', ['sqlite3']])]
    #[TestWith(['oci8', ['oci8']])]
    #[TestWith(['ibm_db2', ['ibm_db2']])]
    #[TestWith(['sqlsrv', ['sqlsrv']])]
    public function test_the_loader_declares_its_drivers_extensions(string $driver, array $expected): void
    {
        static::assertSame(
            $expected,
            (new DbalLoader('t', ['driver' => $driver, 'host' => 'localhost']))->describe()?->extensions(),
        );
    }

    public function test_a_loader_without_a_driver_declares_nothing(): void
    {
        static::assertSame([], (new DbalLoader('t', ['host' => 'localhost']))->describe()?->extensions());
    }
}
