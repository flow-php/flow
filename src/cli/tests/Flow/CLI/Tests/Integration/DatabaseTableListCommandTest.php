<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Name\Identifier;
use Doctrine\DBAL\Schema\Name\UnqualifiedName;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\CLI\Command\DatabaseTableListCommand;
use Flow\CLI\Tests\Context\DatabaseContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Tests\OperatingSystem;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Tester\CommandTester;

final class DatabaseTableListCommandTest extends FlowTestCase
{
    protected ?DatabaseContext $dbContext = null;

    use OperatingSystem;

    protected function setUp(): void
    {
        parent::setUp();

        if ($this->isWindows()) {
            self::markTestSkipped('DatabaseTableListCommand is not supported on Windows.');
        }

        $this->dbContext = new DatabaseContext();
        $this->dbContext->dropAllTables();
    }

    public function test_run_db_table_list(): void
    {
        $this->dbContext()->createTable((new Table('table_01', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
        ]))->addPrimaryKeyConstraint(
            new PrimaryKeyConstraint(null, [new UnqualifiedName(Identifier::unquoted('id'))], true),
        ));

        $this->dbContext()->createTable((new Table('table_02', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('created_at', Type::getType(Types::DATETIME_IMMUTABLE), ['notnull' => true]),
            new Column('tags', Type::getType(Types::JSON), ['notnull' => true, 'platformOptions' => ['jsonb' => true]]),
        ]))->addPrimaryKeyConstraint(
            new PrimaryKeyConstraint(null, [new UnqualifiedName(Identifier::unquoted('id'))], true),
        ));

        $tester = new CommandTester(new DatabaseTableListCommand('db:table:list'));

        $tester->execute([
            '--db-connection-file' => __DIR__ . '/Fixtures/connection.php',
        ]);

        $tester->assertCommandIsSuccessful();

        $display = $tester->getDisplay();

        static::assertStringContainsString(<<<'OUTPUT'
            ┌──────────┬───────────┬─────────┐
            │ Name     │ Namespace │ Columns │
            ├──────────┼───────────┼─────────┤
            │ table_01 │ public    │ 3       │
            │ table_02 │ public    │ 3       │
            └──────────┴───────────┴─────────┘
            OUTPUT, $display);

        static::assertStringContainsString('Summary', $display);
        static::assertStringContainsString('Total tables       2', $display);
        static::assertStringContainsString('Total namespaces   1', $display);
        static::assertStringContainsString('Total columns      6', $display);
    }

    public function test_database_table_list_command_registers_as_db_table_list(): void
    {
        $application = new Application();
        $application->addCommands([new DatabaseTableListCommand()]);

        static::assertInstanceOf(DatabaseTableListCommand::class, $application->find('db:table:list'));
    }

    protected function dbContext(): DatabaseContext
    {
        if (null === $this->dbContext) {
            $this->dbContext = new DatabaseContext();
        }

        return $this->dbContext;
    }
}
