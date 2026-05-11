<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Insert;

use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\InsertStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RawStmt;
use Flow\PostgreSql\QueryBuilder\Clause\ConflictTarget;
use Flow\PostgreSql\QueryBuilder\Clause\OnConflictClause;
use Flow\PostgreSql\QueryBuilder\Condition\IsNull;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\FunctionCall;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use Flow\PostgreSql\QueryBuilder\Insert\InsertBuilder;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\conflict_columns;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\parameters;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;

final class InsertBuilderTest extends TestCase
{
    public function test_builder_chain(): void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name', 'age')
            ->values(Literal::string('john@example.com'), Literal::string('John'), Literal::int(30))
            ->values(Literal::string('jane@example.com'), Literal::string('Jane'), Literal::int(25))
            ->onConflictDoUpdate(ConflictTarget::columns(['email']), [
                'name' => Column::name('excluded.name'),
                'age' => Column::name('excluded.age'),
                'updated_at' => new FunctionCall(['now']),
            ])
            ->where(new IsNull(Column::name('users.deleted_at')))
            ->returning(Column::name('id'), Column::name('email'), Column::name('updated_at'));

        $ast = $query->toAst();

        static::assertInstanceOf(InsertStmt::class, $ast);
        static::assertNotNull($ast->getOnConflictClause());
        static::assertNotNull($ast->getReturningList());
        static::assertCount(3, $ast->getReturningList());
    }

    public function test_immutability_on_columns(): void
    {
        $step1 = InsertBuilder::create()->into('users');
        $step2 = $step1->columns('name', 'email');

        static::assertNotSame($step1, $step2);
    }

    public function test_immutability_on_on_conflict(): void
    {
        $step1 = InsertBuilder::create()->into('users')->columns('email')->values(Literal::string('john@example.com'));
        $step2 = $step1->onConflictDoNothing();

        static::assertNotSame($step1, $step2);
    }

    public function test_immutability_on_returning(): void
    {
        $step1 = InsertBuilder::create()->into('users')->columns('name')->values(Literal::string('John'));
        $step2 = $step1->returning(Column::name('id'));

        static::assertNotSame($step1, $step2);
    }

    public function test_immutability_on_values(): void
    {
        $step1 = InsertBuilder::create()->into('users')->columns('name');
        $step2 = $step1->values(Literal::string('John'));

        static::assertNotSame($step1, $step2);
    }

    public function test_insert_default_values(): void
    {
        $query = InsertBuilder::create()->into('logs')->defaultValues();

        $ast = $query->toAst();

        static::assertInstanceOf(InsertStmt::class, $ast);

        $selectStmtNode = $ast->getSelectStmt();
        static::assertNull($selectStmtNode);
    }

    public function test_insert_into_schema_qualified_table_reference(): void
    {
        static::assertSame(
            "INSERT INTO public.users (name) VALUES ('John')",
            insert()->into(table('users', 'public'))->columns('name')->values(literal('John'))->toSql(),
        );
    }

    public function test_insert_into_table_reference(): void
    {
        static::assertSame(
            "INSERT INTO users (name) VALUES ('John')",
            insert()->into(table('users'))->columns('name')->values(literal('John'))->toSql(),
        );
    }

    public function test_insert_multiple_rows(): void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'))
            ->values(Literal::string('Jane'), Literal::string('jane@example.com'));

        $ast = $query->toAst();

        static::assertInstanceOf(InsertStmt::class, $ast);
        $selectStmt = $ast->getSelectStmt()?->getSelectStmt();
        static::assertNotNull($selectStmt);
        $valuesLists = $selectStmt->getValuesLists();
        static::assertNotNull($valuesLists);
        static::assertCount(2, $valuesLists);
    }

    public function test_insert_multiple_rows_deparsed_output(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $insert = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'))
            ->values(Literal::string('Jane'), Literal::string('jane@example.com'));

        $deparsed = $this->deparse($insert->toAst());
        static::assertSame(
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com'), ('Jane', 'jane@example.com')",
            $deparsed,
        );
    }

    public function test_insert_on_conflict_do_nothing(): void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoNothing(ConflictTarget::columns(['email']));

        $ast = $query->toAst();

        static::assertInstanceOf(InsertStmt::class, $ast);
        static::assertNotNull($ast->getOnConflictClause());
    }

    public function test_insert_on_conflict_do_nothing_without_target(): void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoNothing();

        $ast = $query->toAst();

        static::assertInstanceOf(InsertStmt::class, $ast);
        static::assertNotNull($ast->getOnConflictClause());
    }

    public function test_insert_on_conflict_do_update(): void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoUpdate(ConflictTarget::columns(['email']), ['name' => Column::name('excluded.name')]);

        $ast = $query->toAst();

        static::assertInstanceOf(InsertStmt::class, $ast);
        $onConflict = $ast->getOnConflictClause();
        static::assertNotNull($onConflict);
        static::assertNotNull($onConflict->getTargetList());
        static::assertCount(1, $onConflict->getTargetList());
    }

    public function test_insert_on_conflict_do_update_with_where(): void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoUpdate(ConflictTarget::columns(['email']), ['name' => Column::name('excluded.name')])
            ->where(new IsNull(Column::name('deleted_at')));

        $ast = $query->toAst();

        static::assertInstanceOf(InsertStmt::class, $ast);
        $onConflict = $ast->getOnConflictClause();
        static::assertNotNull($onConflict);
        static::assertNotNull($onConflict->getWhereClause());
    }

    public function test_insert_on_conflict_on_constraint(): void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoNothing(ConflictTarget::constraint('users_email_key'));

        $ast = $query->toAst();

        static::assertInstanceOf(InsertStmt::class, $ast);
        static::assertNotNull($ast->getOnConflictClause());
    }

    public function test_insert_on_conflict_with_clause(): void
    {
        $target = ConflictTarget::columns(['email']);
        $onConflict = OnConflictClause::doUpdate($target, ['name' => Column::name('excluded.name')]);

        $query = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflict($onConflict);

        $ast = $query->toAst();

        static::assertInstanceOf(InsertStmt::class, $ast);
        static::assertNotNull($ast->getOnConflictClause());
    }

    public function test_insert_returning(): void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'))
            ->returning(Column::name('id'), Column::name('created_at'));

        $ast = $query->toAst();

        static::assertInstanceOf(InsertStmt::class, $ast);
        $returning = $ast->getReturningList();
        static::assertNotNull($returning);
        static::assertCount(2, $returning);
    }

    public function test_insert_returning_all(): void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'))
            ->returningAll();

        $ast = $query->toAst();

        static::assertInstanceOf(InsertStmt::class, $ast);
        $returning = $ast->getReturningList();
        static::assertNotNull($returning);
        static::assertCount(1, $returning);
    }

    public function test_insert_select_to_sql(): void
    {
        static::assertSame(
            'INSERT INTO users (name, email) SELECT name, email FROM archived_users',
            insert()
                ->into('users')
                ->columns('name', 'email')
                ->select(select()->select(col('name'), col('email'))->from(table('archived_users')))
                ->toSql(),
        );
    }

    public function test_insert_with_alias(): void
    {
        $query = InsertBuilder::create()->into('users', 'u')->columns('name')->values(Literal::string('John'));

        $ast = $query->toAst();

        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('users', $relation->getRelname());

        $alias = $relation->getAlias();
        static::assertNotNull($alias);
        static::assertSame('u', $alias->getAliasname());
    }

    public function test_insert_with_default_values_to_sql(): void
    {
        static::assertSame('INSERT INTO users DEFAULT VALUES', insert()->into('users')->defaultValues()->toSql());
    }

    public function test_insert_with_function_values(): void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email', 'created_at')
            ->values(Literal::string('John'), Literal::string('john@example.com'), new FunctionCall(['now']));

        $ast = $query->toAst();

        static::assertInstanceOf(InsertStmt::class, $ast);
    }

    public function test_insert_with_function_values_deparsed_output(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $insert = InsertBuilder::create()
            ->into('logs')
            ->columns('message', 'created_at')
            ->values(Literal::string('test message'), new FunctionCall(['now']));

        $deparsed = $this->deparse($insert->toAst());
        static::assertSame("INSERT INTO logs (message, created_at) VALUES ('test message', now())", $deparsed);
    }

    public function test_insert_with_multiple_rows_to_sql(): void
    {
        static::assertSame(
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com'), ('Jane', 'jane@example.com')",
            insert()
                ->into('users')
                ->columns('name', 'email')
                ->values(literal('John'), literal('john@example.com'))
                ->values(literal('Jane'), literal('jane@example.com'))
                ->toSql(),
        );
    }

    public function test_insert_with_on_conflict_do_nothing_deparsed_output(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $insert = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoNothing(ConflictTarget::columns(['email']));

        $deparsed = $this->deparse($insert->toAst());
        static::assertSame(
            "INSERT INTO users (email, name) VALUES ('john@example.com', 'John') ON CONFLICT (email) DO NOTHING",
            $deparsed,
        );
    }

    public function test_insert_with_on_conflict_do_nothing_to_sql(): void
    {
        static::assertSame(
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') ON CONFLICT DO NOTHING",
            insert()
                ->into('users')
                ->columns('name', 'email')
                ->values(literal('John'), literal('john@example.com'))
                ->onConflictDoNothing()
                ->toSql(),
        );
    }

    public function test_insert_with_on_conflict_do_update_deparsed_output(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $insert = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoUpdate(ConflictTarget::columns(['email']), ['name' => Column::tableColumn(
                'excluded',
                'name',
            )]);

        $deparsed = $this->deparse($insert->toAst());
        static::assertSame(
            "INSERT INTO users (email, name) VALUES ('john@example.com', 'John') ON CONFLICT (email) DO UPDATE SET name = excluded.name",
            $deparsed,
        );
    }

    public function test_insert_with_on_conflict_do_update_to_sql(): void
    {
        static::assertSame(
            "INSERT INTO users (email, name) VALUES ('john@example.com', 'John') ON CONFLICT (email) DO UPDATE SET name = 'Updated John'",
            insert()
                ->into('users')
                ->columns('email', 'name')
                ->values(literal('john@example.com'), literal('John'))
                ->onConflictDoUpdate(conflict_columns(['email']), ['name' => literal('Updated John')])
                ->toSql(),
        );
    }

    public function test_insert_with_on_conflict_on_columns_do_nothing_to_sql(): void
    {
        static::assertSame(
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') ON CONFLICT (email) DO NOTHING",
            insert()
                ->into('users')
                ->columns('name', 'email')
                ->values(literal('John'), literal('john@example.com'))
                ->onConflictDoNothing(conflict_columns(['email']))
                ->toSql(),
        );
    }

    public function test_insert_with_parameters_function_multiple_rows_to_sql(): void
    {
        static::assertSame(
            'INSERT INTO users (name, email) VALUES ($1, $2), ($3, $4)',
            insert()
                ->into('users')
                ->columns('name', 'email')
                ->values(...parameters(2))
                ->values(...parameters(2, startAt: 3))
                ->toSql(),
        );
    }

    public function test_insert_with_parameters_function_to_sql(): void
    {
        static::assertSame(
            'INSERT INTO users (name, email) VALUES ($1, $2)',
            insert()->into('users')->columns('name', 'email')->values(...parameters(2))->toSql(),
        );
    }

    public function test_insert_with_parameters_to_sql(): void
    {
        static::assertSame(
            'INSERT INTO users (name, email) VALUES ($1, $2)',
            insert()->into('users')->columns('name', 'email')->values(param(1), param(2))->toSql(),
        );
    }

    public function test_insert_with_returning_all_to_sql(): void
    {
        static::assertSame(
            "INSERT INTO users (name) VALUES ('John') RETURNING *",
            insert()->into('users')->columns('name')->values(literal('John'))->returningAll()->toSql(),
        );
    }

    public function test_insert_with_returning_deparsed_output(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $insert = InsertBuilder::create()
            ->into('users')
            ->columns('name')
            ->values(Literal::string('John'))
            ->returning(Column::name('id'));

        $deparsed = $this->deparse($insert->toAst());
        static::assertSame("INSERT INTO users (name) VALUES ('John') RETURNING id", $deparsed);
    }

    public function test_insert_with_returning_to_sql(): void
    {
        static::assertSame(
            "INSERT INTO users (name) VALUES ('John') RETURNING id",
            insert()->into('users')->columns('name')->values(literal('John'))->returning(col('id'))->toSql(),
        );
    }

    public function test_insert_with_schema(): void
    {
        $query = InsertBuilder::create()->into('public.users')->columns('name')->values(Literal::string('John'));

        $ast = $query->toAst();

        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('users', $relation->getRelname());
        static::assertSame('public', $relation->getSchemaname());
    }

    public function test_insert_with_schema_deparsed_output(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $insert = InsertBuilder::create()->into('public.users')->columns('name')->values(Literal::string('John'));

        $deparsed = $this->deparse($insert->toAst());
        static::assertSame("INSERT INTO public.users (name) VALUES ('John')", $deparsed);
    }

    public function test_insert_with_schema_qualified_table_to_sql(): void
    {
        static::assertSame(
            "INSERT INTO public.users (name) VALUES ('John')",
            insert()->into('public.users')->columns('name')->values(literal('John'))->toSql(),
        );
    }

    public function test_insert_without_columns(): void
    {
        $query = InsertBuilder::create()->into('users')->defaultValues();

        $ast = $query->toAst();

        static::assertInstanceOf(InsertStmt::class, $ast);
        $cols = $ast->getCols();
        static::assertTrue($cols === null || \count($cols) === 0);
    }

    public function test_insert_without_explicit_columns(): void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->values(Literal::string('John'), Literal::string('john@example.com'));

        $ast = $query->toAst();

        static::assertInstanceOf(InsertStmt::class, $ast);
        $cols = $ast->getCols();
        static::assertTrue($cols === null || \count($cols) === 0);
        static::assertNotNull($ast->getSelectStmt());
    }

    public function test_round_trip_default_values(): void
    {
        $original = InsertBuilder::create()->into('logs')->defaultValues();

        $ast = $original->toAst();
        $restored = InsertBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();

        $relation = $restoredAst->getRelation();
        static::assertNotNull($relation);
        static::assertSame('logs', $relation->getRelname());

        static::assertNull($restoredAst->getSelectStmt());
    }

    public function test_round_trip_multiple_rows(): void
    {
        $original = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'))
            ->values(Literal::string('Jane'), Literal::string('jane@example.com'))
            ->values(Literal::string('Bob'), Literal::string('bob@example.com'));

        $ast = $original->toAst();
        $restored = InsertBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();

        $selectStmt = $restoredAst->getSelectStmt()?->getSelectStmt();
        static::assertNotNull($selectStmt);
        $valuesLists = $selectStmt->getValuesLists();
        static::assertNotNull($valuesLists);
        static::assertCount(3, $valuesLists);
    }

    public function test_round_trip_returning_all(): void
    {
        $original = InsertBuilder::create()
            ->into('users')
            ->columns('name')
            ->values(Literal::string('John'))
            ->returningAll();

        $ast = $original->toAst();
        $restored = InsertBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();

        $returning = $restoredAst->getReturningList();
        static::assertNotNull($returning);
        static::assertCount(1, $returning);
    }

    public function test_round_trip_simple(): void
    {
        $original = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'));

        $ast = $original->toAst();
        $restored = InsertBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();

        $originalRelation = $ast->getRelation();
        static::assertNotNull($originalRelation);

        $restoredRelation = $restoredAst->getRelation();
        static::assertNotNull($restoredRelation);

        static::assertSame($originalRelation->getRelname(), $restoredRelation->getRelname());
        static::assertCount(2, $restoredAst->getCols());
    }

    public function test_round_trip_with_on_conflict(): void
    {
        $original = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoUpdate(ConflictTarget::columns(['email']), ['name' => Column::name('excluded.name')]);

        $ast = $original->toAst();
        $restored = InsertBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();

        static::assertNotNull($restoredAst->getOnConflictClause());
        static::assertNotNull($restoredAst->getOnConflictClause()->getTargetList());
    }

    public function test_round_trip_with_returning(): void
    {
        $original = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'))
            ->returning(Column::name('id'));

        $ast = $original->toAst();
        $restored = InsertBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();

        static::assertNotNull($restoredAst->getReturningList());
        static::assertCount(1, $restoredAst->getReturningList());
    }

    public function test_round_trip_with_schema_and_alias(): void
    {
        $original = InsertBuilder::create()
            ->into('public.users', 'u')
            ->columns('name')
            ->values(Literal::string('John'));

        $ast = $original->toAst();
        $restored = InsertBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();

        $relation = $restoredAst->getRelation();
        static::assertNotNull($relation);
        static::assertSame('users', $relation->getRelname());
        static::assertSame('public', $relation->getSchemaname());

        $alias = $relation->getAlias();
        static::assertNotNull($alias);
        static::assertSame('u', $alias->getAliasname());
    }

    public function test_round_trip_with_where_clause(): void
    {
        $original = InsertBuilder::create()
            ->into('users')
            ->columns('email', 'name')
            ->values(Literal::string('john@example.com'), Literal::string('John'))
            ->onConflictDoUpdate(ConflictTarget::columns(['email']), ['name' => Column::name('excluded.name')])
            ->where(new IsNull(Column::name('deleted_at')));

        $ast = $original->toAst();
        $restored = InsertBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();

        static::assertNotNull($restoredAst->getOnConflictClause());
        static::assertNotNull($restoredAst->getOnConflictClause()->getWhereClause());
    }

    public function test_simple_insert(): void
    {
        $query = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'));

        $ast = $query->toAst();

        static::assertInstanceOf(InsertStmt::class, $ast);
        static::assertNotNull($ast->getRelation());
        static::assertSame('users', $ast->getRelation()->getRelname());
        static::assertNotNull($ast->getCols());
        static::assertCount(2, $ast->getCols());
        static::assertNotNull($ast->getSelectStmt());
    }

    public function test_simple_insert_deparsed_output(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $insert = InsertBuilder::create()
            ->into('users')
            ->columns('name', 'email')
            ->values(Literal::string('John'), Literal::string('john@example.com'));

        $deparsed = $this->deparse($insert->toAst());
        static::assertSame("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')", $deparsed);
    }

    public function test_simple_insert_with_values_to_sql(): void
    {
        static::assertSame(
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
            insert()
                ->into('users')
                ->columns('name', 'email')
                ->values(literal('John'), literal('john@example.com'))
                ->toSql(),
        );
    }

    private function deparse(InsertStmt $insertStmt): string
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
