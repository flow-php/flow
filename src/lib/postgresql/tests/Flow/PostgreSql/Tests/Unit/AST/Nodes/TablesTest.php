<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Nodes;

use Flow\PostgreSql\AST\Nodes\{Table, Tables};
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use PHPUnit\Framework\TestCase;

final class TablesTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_all_returns_all_tables() : void
    {
        $table1 = new Table($this->createRangeVar('users'));
        $table2 = new Table($this->createRangeVar('orders'));

        $tables = new Tables([$table1, $table2]);

        self::assertSame([$table1, $table2], $tables->all());
    }

    public function test_count_returns_number_of_tables() : void
    {
        $tables = new Tables([
            new Table($this->createRangeVar('users')),
            new Table($this->createRangeVar('orders')),
        ]);

        self::assertCount(2, $tables);
    }

    public function test_count_returns_zero_for_empty_collection() : void
    {
        $tables = new Tables([]);

        self::assertCount(0, $tables);
    }

    public function test_first_returns_first_table() : void
    {
        $table1 = new Table($this->createRangeVar('users'));
        $table2 = new Table($this->createRangeVar('orders'));

        $tables = new Tables([$table1, $table2]);

        self::assertSame($table1, $tables->first());
    }

    public function test_first_returns_null_for_empty_collection() : void
    {
        $tables = new Tables([]);

        self::assertNull($tables->first());
    }

    public function test_get_returns_null_for_invalid_index() : void
    {
        $tables = new Tables([new Table($this->createRangeVar('users'))]);

        self::assertNull($tables->get(5));
    }

    public function test_get_returns_table_at_index() : void
    {
        $table1 = new Table($this->createRangeVar('users'));
        $table2 = new Table($this->createRangeVar('orders'));

        $tables = new Tables([$table1, $table2]);

        self::assertSame($table1, $tables->get(0));
        self::assertSame($table2, $tables->get(1));
    }

    public function test_is_empty_returns_false_for_non_empty_collection() : void
    {
        $tables = new Tables([new Table($this->createRangeVar('users'))]);

        self::assertFalse($tables->isEmpty());
    }

    public function test_is_empty_returns_true_for_empty_collection() : void
    {
        $tables = new Tables([]);

        self::assertTrue($tables->isEmpty());
    }

    public function test_is_iterable() : void
    {
        $table1 = new Table($this->createRangeVar('users'));
        $table2 = new Table($this->createRangeVar('orders'));

        $tables = new Tables([$table1, $table2]);

        $result = [];

        foreach ($tables as $table) {
            $result[] = $table;
        }

        self::assertSame([$table1, $table2], $result);
    }

    public function test_is_single_returns_false_for_empty_collection() : void
    {
        $tables = new Tables([]);

        self::assertFalse($tables->isSingle());
    }

    public function test_is_single_returns_false_for_multiple_tables() : void
    {
        $tables = new Tables([
            new Table($this->createRangeVar('users')),
            new Table($this->createRangeVar('orders')),
        ]);

        self::assertFalse($tables->isSingle());
    }

    public function test_is_single_returns_true_for_single_table() : void
    {
        $tables = new Tables([new Table($this->createRangeVar('users'))]);

        self::assertTrue($tables->isSingle());
    }

    public function test_last_returns_last_table() : void
    {
        $table1 = new Table($this->createRangeVar('users'));
        $table2 = new Table($this->createRangeVar('orders'));

        $tables = new Tables([$table1, $table2]);

        self::assertSame($table2, $tables->last());
    }

    public function test_last_returns_null_for_empty_collection() : void
    {
        $tables = new Tables([]);

        self::assertNull($tables->last());
    }

    private function createRangeVar(string $tableName) : RangeVar
    {
        $rangeVar = new RangeVar();
        $rangeVar->setRelname($tableName);

        return $rangeVar;
    }
}
