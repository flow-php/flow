<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    alter,
    create,
    data_type_integer,
    drop,
    func_arg
};
use Flow\PostgreSql\QueryBuilder\Schema\Function\ParallelSafety;

final class FunctionDatabaseTest extends DatabaseTestCase
{
    private const FUNCTION_NAME = 'flow_postgres_test_func';

    private const FUNCTION_NAME_2 = 'flow_postgres_test_func_2';

    private const FUNCTION_NAME_RENAMED = 'flow_postgres_test_func_renamed';

    protected function tearDown() : void
    {
        $this->dropFunctionIfExists(self::FUNCTION_NAME);
        $this->dropFunctionIfExists(self::FUNCTION_NAME_2);
        $this->dropFunctionIfExists(self::FUNCTION_NAME_RENAMED);

        parent::tearDown();
    }

    public function test_alter_function_rename() : void
    {
        $this->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returns(data_type_integer())
                ->language('sql')
                ->as('SELECT 42')
                ->toSql()
        );

        $result = $this->execute(
            alter()->function(self::FUNCTION_NAME)
                ->arguments()
                ->renameTo(self::FUNCTION_NAME_RENAMED)
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertFalse($this->functionExists(self::FUNCTION_NAME));
        self::assertTrue($this->functionExists(self::FUNCTION_NAME_RENAMED));
    }

    public function test_alter_function_set_volatility() : void
    {
        $this->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returns(data_type_integer())
                ->language('sql')
                ->as('SELECT 42')
                ->toSql()
        );

        $result = $this->execute(
            alter()->function(self::FUNCTION_NAME)
                ->arguments()
                ->immutable()
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->functionExists(self::FUNCTION_NAME));
    }

    public function test_create_function_returns_void() : void
    {
        $result = $this->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returnsVoid()
                ->language('sql')
                ->as('SELECT 1')
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->functionExists(self::FUNCTION_NAME));
    }

    public function test_create_function_with_arguments() : void
    {
        $result = $this->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments(
                    func_arg(data_type_integer())->named('a'),
                    func_arg(data_type_integer())->named('b')
                )
                ->returns(data_type_integer())
                ->language('sql')
                ->as('SELECT a + b')
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->functionExists(self::FUNCTION_NAME));

        $row = $this->fetchOne($this->execute('SELECT ' . self::FUNCTION_NAME . '(10, 5) AS result'));
        self::assertSame('15', $row['result']);
    }

    public function test_create_function_with_default_argument() : void
    {
        $result = $this->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments(
                    func_arg(data_type_integer())->named('a'),
                    func_arg(data_type_integer())->named('b')->default('10')
                )
                ->returns(data_type_integer())
                ->language('sql')
                ->as('SELECT a + b')
                ->toSql()
        );

        self::assertNotFalse($result);

        $row = $this->fetchOne($this->execute('SELECT ' . self::FUNCTION_NAME . '(5) AS result'));
        self::assertSame('15', $row['result']);

        $row = $this->fetchOne($this->execute('SELECT ' . self::FUNCTION_NAME . '(5, 3) AS result'));
        self::assertSame('8', $row['result']);
    }

    public function test_create_function_with_parallel_safety() : void
    {
        $result = $this->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returns(data_type_integer())
                ->language('sql')
                ->parallel(ParallelSafety::SAFE)
                ->as('SELECT 42')
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->functionExists(self::FUNCTION_NAME));
    }

    public function test_create_function_with_volatility() : void
    {
        $result = $this->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returns(data_type_integer())
                ->language('sql')
                ->immutable()
                ->as('SELECT 42')
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->functionExists(self::FUNCTION_NAME));
    }

    public function test_create_or_replace_function() : void
    {
        $this->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returns(data_type_integer())
                ->language('sql')
                ->as('SELECT 1')
                ->toSql()
        );

        $row = $this->fetchOne($this->execute('SELECT ' . self::FUNCTION_NAME . '() AS result'));
        self::assertSame('1', $row['result']);

        $result = $this->execute(
            create()->function(self::FUNCTION_NAME)
                ->orReplace()
                ->arguments()
                ->returns(data_type_integer())
                ->language('sql')
                ->as('SELECT 2')
                ->toSql()
        );

        self::assertNotFalse($result);

        $row = $this->fetchOne($this->execute('SELECT ' . self::FUNCTION_NAME . '() AS result'));
        self::assertSame('2', $row['result']);
    }

    public function test_create_simple_function() : void
    {
        $result = $this->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returns(data_type_integer())
                ->language('sql')
                ->as('SELECT 42')
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->functionExists(self::FUNCTION_NAME));

        $row = $this->fetchOne($this->execute('SELECT ' . self::FUNCTION_NAME . '() AS result'));
        self::assertSame('42', $row['result']);
    }

    public function test_drop_function() : void
    {
        $this->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returns(data_type_integer())
                ->language('sql')
                ->as('SELECT 42')
                ->toSql()
        );

        self::assertTrue($this->functionExists(self::FUNCTION_NAME));

        $result = $this->execute(
            drop()->function(self::FUNCTION_NAME)->toSql()
        );

        self::assertNotFalse($result);
        self::assertFalse($this->functionExists(self::FUNCTION_NAME));
    }

    public function test_drop_function_if_exists() : void
    {
        $result = $this->execute(
            drop()->function(self::FUNCTION_NAME)->ifExists()->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_drop_function_with_arguments() : void
    {
        $this->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments(func_arg(data_type_integer())->named('a'))
                ->returns(data_type_integer())
                ->language('sql')
                ->as('SELECT a * 2')
                ->toSql()
        );

        $result = $this->execute(
            drop()->function(self::FUNCTION_NAME)
                ->arguments(func_arg(data_type_integer()))
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertFalse($this->functionExists(self::FUNCTION_NAME));
    }

    protected function dropFunctionIfExists(string $name) : void
    {
        $this->execute("DROP FUNCTION IF EXISTS {$name} CASCADE");
    }

    protected function functionExists(string $name) : bool
    {
        $row = $this->fetchOne(
            $this->execute("SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = '{$name}') AS func_exists")
        );

        return $row['func_exists'] === 't';
    }
}
