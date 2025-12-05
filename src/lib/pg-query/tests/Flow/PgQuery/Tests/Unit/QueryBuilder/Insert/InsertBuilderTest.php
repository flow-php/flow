<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Insert;

use Flow\PgQuery\{ParsedQuery, Parser};
use Flow\PgQuery\Protobuf\AST\{InsertStmt, Node, RawStmt};
use Flow\PgQuery\QueryBuilder\Clause\{ConflictTarget, OnConflictClause};
use Flow\PgQuery\QueryBuilder\Condition\IsNull;
use Flow\PgQuery\QueryBuilder\Expression\{Column, FunctionCall, Literal};
use Flow\PgQuery\QueryBuilder\Insert\InsertBuilder;
use PHPUnit\Framework\TestCase;

final class InsertBuilderTest extends TestCase
{
    public function test_builder_chain() : void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name', 'age')
            ->values(Literal::string('john@example.com'), Literal::string('John'), Literal::int(30))
            ->values(Literal::string('jane@example.com'), Literal::string('Jane'), Literal::int(25))
            ->onConflictDoUpdate(
                ConflictTarget::columns(['email']),
                [
                    'name' => Column::name('excluded.name'),
                    'age' => Column::name('excluded.age'),
                    'updated_at' => new FunctionCall(['now']),
                ]
            )
            ->where(new IsNull(Column::name('users.deleted_at')))
            ->returning(Column::name('id'), Column::name('email'), Column::name('updated_at'));

        $ast = $query->toAst();

        self::assertInstanceOf(InsertStmt::class, $ast);
        self::assertNotNull($ast->getOnConflictClause());
        self::assertNotNull($ast->getReturningList());
        self::assertCount(3, $ast->getReturningList());
    }

    public function test_immutability_on_columns() : void
    {
        $step1 = InsertBuilder::create()->into('users');
        $step2 = $step1->columns('name', 'email');

        self::assertNotSame($step1, $step2);
    }

    public function test_immutability_on_on_conflict() : void
    {
        $step1 = InsertBuilder::create()
            ->into('users')
            ->columns('email')
            ->values(Literal::string('john@example.com'));
        $step2 = $step1->onConflictDoNothing();

        self::assertNotSame($step1, $step2);
    }

    public function test_immutability_on_returning() : void
    {
        $step1 = InsertBuilder::create()
            ->into('users')
            ->columns('name')
            ->values(Literal::string('John'));
        $step2 = $step1->returning(Column::name('id'));

        self::assertNotSame($step1, $step2);
    }

    public function test_immutability_on_values() : void
    {
        $step1 = InsertBuilder::create()
            ->into('users')
            ->columns('name');
        $step2 = $step1->values(Literal::string('John'));

        self::assertNotSame($step1, $step2);
    }

    public function test_insert_default_values() : void
    {
        $query = InsertBuilder::create()
            ->into('logs')
            ->defaultValues();

        $ast = $query->toAst();

        self::assertInstanceOf(InsertStmt::class, $ast);

        // DEFAULT VALUES produces an INSERT with no selectStmt at all
        // This is the correct PostgreSQL AST representation for DEFAULT VALUES
        $selectStmtNode = $ast->getSelectStmt();
        self::assertNull($selectStmtNode);
    }

    public function test_insert_multiple_rows() : void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'))
            ->values(Literal::string('Jane'), Literal::string('jane@example.com'));

        $ast = $query->toAst();

        self::assertInstanceOf(InsertStmt::class, $ast);
        $selectStmt = $ast->getSelectStmt()?->getSelectStmt();
        self::assertNotNull($selectStmt);
        $valuesLists = $selectStmt->getValuesLists();
        self::assertNotNull($valuesLists);
        self::assertCount(2, $valuesLists);
    }

    public function test_insert_multiple_rows_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $insert = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'))
            ->values(Literal::string('Jane'), Literal::string('jane@example.com'));

        $deparsed = $this->deparse($insert->toAst());
        self::assertSame("INSERT INTO users (name, email) VALUES ('John', 'john@example.com'), ('Jane', 'jane@example.com')", $deparsed);
    }

    public function test_insert_on_conflict_do_nothing() : void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoNothing(ConflictTarget::columns(['email']));

        $ast = $query->toAst();

        self::assertInstanceOf(InsertStmt::class, $ast);
        self::assertNotNull($ast->getOnConflictClause());
    }

    public function test_insert_on_conflict_do_nothing_without_target() : void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoNothing();

        $ast = $query->toAst();

        self::assertInstanceOf(InsertStmt::class, $ast);
        self::assertNotNull($ast->getOnConflictClause());
    }

    public function test_insert_on_conflict_do_update() : void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoUpdate(
                ConflictTarget::columns(['email']),
                ['name' => Column::name('excluded.name')]
            );

        $ast = $query->toAst();

        self::assertInstanceOf(InsertStmt::class, $ast);
        $onConflict = $ast->getOnConflictClause();
        self::assertNotNull($onConflict);
        self::assertNotNull($onConflict->getTargetList());
        self::assertCount(1, $onConflict->getTargetList());
    }

    public function test_insert_on_conflict_do_update_with_where() : void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoUpdate(
                ConflictTarget::columns(['email']),
                ['name' => Column::name('excluded.name')]
            )
            ->where(new IsNull(Column::name('deleted_at')));

        $ast = $query->toAst();

        self::assertInstanceOf(InsertStmt::class, $ast);
        $onConflict = $ast->getOnConflictClause();
        self::assertNotNull($onConflict);
        self::assertNotNull($onConflict->getWhereClause());
    }

    public function test_insert_on_conflict_on_constraint() : void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoNothing(ConflictTarget::constraint('users_email_key'));

        $ast = $query->toAst();

        self::assertInstanceOf(InsertStmt::class, $ast);
        self::assertNotNull($ast->getOnConflictClause());
    }

    public function test_insert_on_conflict_with_clause() : void
    {
        $target = ConflictTarget::columns(['email']);
        $onConflict = OnConflictClause::doUpdate($target, ['name' => Column::name('excluded.name')]);

        $query = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflict($onConflict);

        $ast = $query->toAst();

        self::assertInstanceOf(InsertStmt::class, $ast);
        self::assertNotNull($ast->getOnConflictClause());
    }

    public function test_insert_returning() : void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'))
            ->returning(Column::name('id'), Column::name('created_at'));

        $ast = $query->toAst();

        self::assertInstanceOf(InsertStmt::class, $ast);
        $returning = $ast->getReturningList();
        self::assertNotNull($returning);
        self::assertCount(2, $returning);
    }

    public function test_insert_returning_all() : void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'))
            ->returningAll();

        $ast = $query->toAst();

        self::assertInstanceOf(InsertStmt::class, $ast);
        $returning = $ast->getReturningList();
        self::assertNotNull($returning);
        self::assertCount(1, $returning);
    }

    public function test_insert_with_alias() : void
    {
        $query = InsertBuilder::create()
            ->into('users', 'u')
            ->columns('name')
            ->values(Literal::string('John'));

        $ast = $query->toAst();

        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('users', $relation->getRelname());

        $alias = $relation->getAlias();
        self::assertNotNull($alias);
        self::assertSame('u', $alias->getAliasname());
    }

    public function test_insert_with_function_values() : void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email', 'created_at')
            ->values(
                Literal::string('John'),
                Literal::string('john@example.com'),
                new FunctionCall(['now'])
            );

        $ast = $query->toAst();

        self::assertInstanceOf(InsertStmt::class, $ast);
    }

    public function test_insert_with_function_values_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $insert = InsertBuilder::create()
            ->into('logs')
            ->columns('message', 'created_at')
            ->values(Literal::string('test message'), new FunctionCall(['now']));

        $deparsed = $this->deparse($insert->toAst());
        self::assertSame("INSERT INTO logs (message, created_at) VALUES ('test message', now())", $deparsed);
    }

    public function test_insert_with_on_conflict_do_nothing_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $insert = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoNothing(ConflictTarget::columns(['email']));

        $deparsed = $this->deparse($insert->toAst());
        self::assertSame("INSERT INTO users (email, name) VALUES ('john@example.com', 'John') ON CONFLICT (email) DO NOTHING", $deparsed);
    }

    public function test_insert_with_on_conflict_do_update_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $insert = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoUpdate(
                ConflictTarget::columns(['email']),
                ['name' => Column::tableColumn('excluded', 'name')]
            );

        $deparsed = $this->deparse($insert->toAst());
        self::assertSame("INSERT INTO users (email, name) VALUES ('john@example.com', 'John') ON CONFLICT (email) DO UPDATE SET name = excluded.name", $deparsed);
    }

    public function test_insert_with_returning_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $insert = InsertBuilder::create()
            ->into('users')
            ->columns('name')
            ->values(Literal::string('John'))
            ->returning(Column::name('id'));

        $deparsed = $this->deparse($insert->toAst());
        self::assertSame("INSERT INTO users (name) VALUES ('John') RETURNING id", $deparsed);
    }

    public function test_insert_with_schema() : void
    {
        $query = InsertBuilder::create()
            ->into('public.users')
            ->columns('name')
            ->values(Literal::string('John'));

        $ast = $query->toAst();

        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('users', $relation->getRelname());
        self::assertSame('public', $relation->getSchemaname());
    }

    public function test_insert_with_schema_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $insert = InsertBuilder::create()
            ->into('public.users')
            ->columns('name')
            ->values(Literal::string('John'));

        $deparsed = $this->deparse($insert->toAst());
        self::assertSame("INSERT INTO public.users (name) VALUES ('John')", $deparsed);
    }

    public function test_insert_without_columns() : void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->defaultValues();

        $ast = $query->toAst();

        self::assertInstanceOf(InsertStmt::class, $ast);
        $cols = $ast->getCols();
        self::assertTrue($cols === null || \count($cols) === 0);
    }

    public function test_insert_without_explicit_columns() : void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->values(Literal::string('John'), Literal::string('john@example.com'));

        $ast = $query->toAst();

        self::assertInstanceOf(InsertStmt::class, $ast);
        $cols = $ast->getCols();
        self::assertTrue($cols === null || \count($cols) === 0);
        self::assertNotNull($ast->getSelectStmt());
    }

    public function test_round_trip_default_values() : void
    {
        $original = InsertBuilder::create()
            ->into('logs')
            ->defaultValues();

        $ast = $original->toAst();
        $node = new Node();
        $node->setInsertStmt($ast);
        $restored = InsertBuilder::fromAst($node);

        $restoredAst = $restored->toAst();

        $relation = $restoredAst->getRelation();
        self::assertNotNull($relation);
        self::assertSame('logs', $relation->getRelname());

        // DEFAULT VALUES has no selectStmt - this is correct PostgreSQL AST
        self::assertNull($restoredAst->getSelectStmt());
    }

    public function test_round_trip_multiple_rows() : void
    {
        $original = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'))
            ->values(Literal::string('Jane'), Literal::string('jane@example.com'))
            ->values(Literal::string('Bob'), Literal::string('bob@example.com'));

        $ast = $original->toAst();
        $node = new Node();
        $node->setInsertStmt($ast);
        $restored = InsertBuilder::fromAst($node);

        $restoredAst = $restored->toAst();

        $selectStmt = $restoredAst->getSelectStmt()?->getSelectStmt();
        self::assertNotNull($selectStmt);
        $valuesLists = $selectStmt->getValuesLists();
        self::assertNotNull($valuesLists);
        self::assertCount(3, $valuesLists);
    }

    public function test_round_trip_returning_all() : void
    {
        $original = InsertBuilder::create()
            ->into('users')
            ->columns('name')
            ->values(Literal::string('John'))
            ->returningAll();

        $ast = $original->toAst();
        $node = new Node();
        $node->setInsertStmt($ast);
        $restored = InsertBuilder::fromAst($node);

        $restoredAst = $restored->toAst();

        $returning = $restoredAst->getReturningList();
        self::assertNotNull($returning);
        self::assertCount(1, $returning);
    }

    public function test_round_trip_simple() : void
    {
        $original = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'));

        $ast = $original->toAst();
        $node = new Node();
        $node->setInsertStmt($ast);
        $restored = InsertBuilder::fromAst($node);

        $restoredAst = $restored->toAst();

        $originalRelation = $ast->getRelation();
        self::assertNotNull($originalRelation);

        $restoredRelation = $restoredAst->getRelation();
        self::assertNotNull($restoredRelation);

        self::assertSame($originalRelation->getRelname(), $restoredRelation->getRelname());
        self::assertCount(2, $restoredAst->getCols());
    }

    public function test_round_trip_with_on_conflict() : void
    {
        $original = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoUpdate(
                ConflictTarget::columns(['email']),
                ['name' => Column::name('excluded.name')]
            );

        $ast = $original->toAst();
        $node = new Node();
        $node->setInsertStmt($ast);
        $restored = InsertBuilder::fromAst($node);

        $restoredAst = $restored->toAst();

        self::assertNotNull($restoredAst->getOnConflictClause());
        self::assertNotNull($restoredAst->getOnConflictClause()->getTargetList());
    }

    public function test_round_trip_with_returning() : void
    {
        $original = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'))
            ->returning(Column::name('id'));

        $ast = $original->toAst();
        $node = new Node();
        $node->setInsertStmt($ast);
        $restored = InsertBuilder::fromAst($node);

        $restoredAst = $restored->toAst();

        self::assertNotNull($restoredAst->getReturningList());
        self::assertCount(1, $restoredAst->getReturningList());
    }

    public function test_round_trip_with_schema_and_alias() : void
    {
        $original = InsertBuilder::create()
            ->into('public.users', 'u')
            ->columns('name')
            ->values(Literal::string('John'));

        $ast = $original->toAst();
        $node = new Node();
        $node->setInsertStmt($ast);
        $restored = InsertBuilder::fromAst($node);

        $restoredAst = $restored->toAst();

        $relation = $restoredAst->getRelation();
        self::assertNotNull($relation);
        self::assertSame('users', $relation->getRelname());
        self::assertSame('public', $relation->getSchemaname());

        $alias = $relation->getAlias();
        self::assertNotNull($alias);
        self::assertSame('u', $alias->getAliasname());
    }

    public function test_round_trip_with_where_clause() : void
    {
        $original = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoUpdate(
                ConflictTarget::columns(['email']),
                ['name' => Column::name('excluded.name')]
            )
            ->where(new IsNull(Column::name('deleted_at')));

        $ast = $original->toAst();
        $node = new Node();
        $node->setInsertStmt($ast);
        $restored = InsertBuilder::fromAst($node);

        $restoredAst = $restored->toAst();

        self::assertNotNull($restoredAst->getOnConflictClause());
        self::assertNotNull($restoredAst->getOnConflictClause()->getWhereClause());
    }

    public function test_simple_insert() : void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'));

        $ast = $query->toAst();

        self::assertInstanceOf(InsertStmt::class, $ast);
        self::assertNotNull($ast->getRelation());
        self::assertSame('users', $ast->getRelation()->getRelname());
        self::assertNotNull($ast->getCols());
        self::assertCount(2, $ast->getCols());
        self::assertNotNull($ast->getSelectStmt());
    }

    public function test_simple_insert_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $insert = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'));

        $deparsed = $this->deparse($insert->toAst());
        self::assertSame("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')", $deparsed);
    }

    private function deparse(InsertStmt $insertStmt) : string
    {
        $parser = new Parser();
        $node = new Node();
        $node->setInsertStmt($insertStmt);
        $rawStmt = new RawStmt();
        $rawStmt->setStmt($node);
        $parsed = $parser->parse('INSERT INTO dummy VALUES (1)');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        return (new ParsedQuery($parseResult))->deparse();
    }
}
