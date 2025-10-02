<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration;

use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\CLI\Command\{DatabaseTableListCommand};
use Flow\CLI\Tests\Context\DatabaseContext;
use Flow\ETL\Tests\FlowTestCase;
use Symfony\Component\Console\Tester\CommandTester;
use Flow\Filesystem\Tests\OperatingSystem;

final class DatabaseTableListCommandTest extends FlowTestCase
{
    protected ?DatabaseContext $dbContext = null;

    use OperatingSystem;

    protected function setUp() : void
    {
        parent::setUp();

        if ($this->isWindows()) {
            self::markTestSkipped('DatabaseTableListCommand is not supported on Windows.');
        }

        $this->dbContext = new DatabaseContext();
        $this->dbContext->dropAllTables();
    }

    public function test_run_db_table_list() : void
    {
        $this->dbContext()->createTable(
            (new Table(
                'table_01',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                ],
            ))->setPrimaryKey(['id'])
        );

        $this->dbContext()->createTable(
            (new Table(
                'table_02',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('created_at', Type::getType(Types::DATETIME_IMMUTABLE), ['notnull' => true]),
                    new Column('tags', Type::getType(Types::JSON), ['notnull' => true, 'platformOptions' => ['jsonb' => true]]),
                ],
            ))->setPrimaryKey(['id'])
        );

        $tester = new CommandTester(new DatabaseTableListCommand('db:table:list'));

        $tester->execute([
            '--db-connection-file' => __DIR__ . '/Fixtures/connection.php',
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertSame(
            <<<'OUTPUT'
┌──────────┬───────────┬─────────┐
│ Name     │ Namespace │ Columns │
├──────────┼───────────┼─────────┤
│ table_01 │ public    │ 3       │
│ table_02 │ public    │ 3       │
└──────────┴───────────┴─────────┘
 ------------------ ----- 
  Summary                 
 ------------------ ----- 
  Total tables       2    
  Total namespaces   1    
  Total columns      6    
 ------------------ ----- 


OUTPUT,
            $tester->getDisplay()
        );
    }

    protected function dbContext() : DatabaseContext
    {
        if (null === $this->dbContext) {
            $this->dbContext = new DatabaseContext();
        }

        return $this->dbContext;
    }
}
