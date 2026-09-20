<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\TransactionIsolationLevel;
use Flow\ETL\Adapter\Doctrine\IsolationLevelRestore;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class IsolationLevelRestoreTest extends TestCase
{
    public function test_restore_sets_the_previous_level_back(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects(self::once())
            ->method('setTransactionIsolation')
            ->with(TransactionIsolationLevel::READ_COMMITTED);

        (new IsolationLevelRestore($connection, TransactionIsolationLevel::READ_COMMITTED))->restore();
    }

    public function test_a_failing_restore_is_suppressed(): void
    {
        $connection = $this->createStub(Connection::class);
        $connection->method('setTransactionIsolation')->willThrowException(new RuntimeException('restore failed'));

        (new IsolationLevelRestore($connection, TransactionIsolationLevel::READ_COMMITTED))->restore();

        $this->addToAssertionCount(1);
    }
}
