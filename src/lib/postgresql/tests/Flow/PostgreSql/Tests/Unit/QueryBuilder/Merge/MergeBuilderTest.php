<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Merge;

use function Flow\PostgreSql\DSL\{col, cte, eq, gt, literal, merge, param, select, table, with};

use Flow\PostgreSql\{ParsedQuery, Parser};
use Flow\PostgreSql\Protobuf\AST\{MergeStmt, Node, RawStmt, SelectStmt};
use Flow\PostgreSql\QueryBuilder\Clause\{CTE, WithClause};
use Flow\PostgreSql\QueryBuilder\Condition\{Comparison, ComparisonOperator};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\Expression\{Column, Literal};
use Flow\PostgreSql\QueryBuilder\Merge\{MergeActionType, MergeBuilder, MergeFinalStep, MergeMatchKind, MergeWhenClauseData, MergeWhenMatched, MergeWhenNotMatched};
use Flow\PostgreSql\QueryBuilder\Select\SelectBuilder;
use Flow\PostgreSql\QueryBuilder\Table\Table;
use PHPUnit\Framework\TestCase;

final class MergeBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_builder_steps_allow_fluent_interface() : void
    {
        $query = MergeBuilder::create()
            ->into('users')
            ->using('new_users', 'n')
            ->on(new Comparison(Column::tableColumn('users', 'id'), ComparisonOperator::EQ, Column::tableColumn('n', 'id')))
            ->whenMatched()
            ->thenUpdate(['name' => Column::tableColumn('n', 'name')]);

        $ast = $query->toAst();
        self::assertInstanceOf(MergeStmt::class, $ast);
    }

    public function test_immutability_into() : void
    {
        $original = MergeBuilder::create();
        $modified = $original->into('users');

        self::assertNotSame($original, $modified);
    }

    public function test_immutability_on() : void
    {
        $original = MergeBuilder::create()->into('users')->using('source', 's');
        $modified = $original->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')));

        self::assertNotSame($original, $modified);
    }

    public function test_immutability_using() : void
    {
        $original = MergeBuilder::create()->into('users');
        $modified = $original->using('source', 's');

        self::assertNotSame($original, $modified);
    }

    public function test_immutability_when_clause() : void
    {
        $builder = MergeBuilder::create()
            ->into('users')
            ->using('source', 's')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')));

        $original = $builder->whenMatched()->thenUpdate(['name' => Literal::string('test')]);
        $modified = $original->whenNotMatched()->thenDoNothing();

        self::assertNotSame($original, $modified);
    }

    public function test_merge_action_type_enum_values() : void
    {
        self::assertSame(5, MergeActionType::DELETE->value);
        self::assertSame(8, MergeActionType::DO_NOTHING->value);
        self::assertSame(4, MergeActionType::INSERT->value);
        self::assertSame(3, MergeActionType::UPDATE->value);
    }

    public function test_merge_deparsed_simple_update() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $merge = MergeBuilder::create()
            ->into('target')
            ->using('source', 's')
            ->on(new Comparison(Column::tableColumn('target', 'id'), ComparisonOperator::EQ, Column::tableColumn('s', 'id')))
            ->whenMatched()
            ->thenUpdate(['value' => Column::tableColumn('s', 'value')]);

        $deparsed = $this->deparse($merge->toAst());
        self::assertSame('MERGE INTO target USING source s ON target.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value', $deparsed);
    }

    public function test_merge_deparsed_with_alias() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $merge = MergeBuilder::create()
            ->into('target', 't')
            ->using('source', 's')
            ->on(new Comparison(Column::tableColumn('t', 'id'), ComparisonOperator::EQ, Column::tableColumn('s', 'id')))
            ->whenMatched()
            ->thenDelete();

        $deparsed = $this->deparse($merge->toAst());
        self::assertSame('MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN DELETE', $deparsed);
    }

    public function test_merge_into_parses_schema_and_table() : void
    {
        $query = MergeBuilder::create()
            ->into('myschema.users', 'u')
            ->using('source', 's')
            ->on(new Comparison(Column::tableColumn('u', 'id'), ComparisonOperator::EQ, Column::tableColumn('s', 'id')))
            ->whenMatched()
            ->thenDoNothing();

        $ast = $query->toAst();

        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('users', $relation->getRelname());
        self::assertSame('myschema', $relation->getSchemaname());

        $alias = $relation->getAlias();
        self::assertNotNull($alias);
        self::assertSame('u', $alias->getAliasname());
    }

    public function test_merge_match_kind_enum_values() : void
    {
        self::assertSame(1, MergeMatchKind::MATCHED->value);
        self::assertSame(2, MergeMatchKind::NOT_MATCHED_BY_SOURCE->value);
        self::assertSame(3, MergeMatchKind::NOT_MATCHED_BY_TARGET->value);
    }

    public function test_merge_simple_update_to_sql() : void
    {
        self::assertSame(
            'MERGE INTO target USING source s ON target.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value',
            merge('target')
                ->using('source', 's')
                ->on(eq(col('target.id'), col('s.id')))
                ->whenMatched()
                ->thenUpdate([
                    'value' => col('s.value'),
                ])
                ->toSql()
        );
    }

    public function test_merge_using_parses_schema_and_table() : void
    {
        $query = MergeBuilder::create()
            ->into('target', 't')
            ->using('myschema.source', 's')
            ->on(new Comparison(Column::tableColumn('t', 'id'), ComparisonOperator::EQ, Column::tableColumn('s', 'id')))
            ->whenMatched()
            ->thenDoNothing();

        $ast = $query->toAst();

        $sourceRelation = $ast->getSourceRelation();
        self::assertNotNull($sourceRelation);

        $rangeVar = $sourceRelation->getRangeVar();
        self::assertNotNull($rangeVar);
        self::assertSame('source', $rangeVar->getRelname());
        self::assertSame('myschema', $rangeVar->getSchemaname());

        $alias = $rangeVar->getAlias();
        self::assertNotNull($alias);
        self::assertSame('s', $alias->getAliasname());
    }

    public function test_merge_using_subquery_to_sql() : void
    {
        self::assertSame(
            'MERGE INTO users USING (SELECT id, name, email FROM staged_data) src ON users.id = src.id WHEN MATCHED THEN UPDATE SET name = src.name, email = src.email',
            merge('users')
                ->using(
                    select()
                        ->select(col('id'), col('name'), col('email'))
                        ->from(table('staged_data')),
                    'src'
                )
                ->on(eq(col('users.id'), col('src.id')))
                ->whenMatched()
                ->thenUpdate([
                    'name' => col('src.name'),
                    'email' => col('src.email'),
                ])
                ->toSql()
        );
    }

    public function test_merge_when_clause_data_structure() : void
    {
        $condition = new Comparison(Column::name('id'), ComparisonOperator::EQ, Literal::int(1));
        $assignments = ['name' => Literal::string('test')];
        $columns = ['id', 'name'];
        $values = [Literal::int(1), Literal::string('test')];

        $data = new MergeWhenClauseData(
            MergeMatchKind::MATCHED,
            MergeActionType::UPDATE,
            $condition,
            $assignments,
            $columns,
            $values
        );

        self::assertSame(MergeMatchKind::MATCHED, $data->matchKind);
        self::assertSame(MergeActionType::UPDATE, $data->actionType);
        self::assertSame($condition, $data->condition);
        self::assertSame($assignments, $data->assignments);
        self::assertSame($columns, $data->insertColumns);
        self::assertSame($values, $data->insertValues);
    }

    public function test_merge_when_matched_delete() : void
    {
        $query = MergeBuilder::create()
            ->into('target')
            ->using('source', 's')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')))
            ->whenMatched()
            ->thenDelete();

        $ast = $query->toAst();

        $whenClauses = $ast->getMergeWhenClauses();
        self::assertCount(1, $whenClauses);

        $whenClause = $whenClauses[0]->getMergeWhenClause();
        self::assertNotNull($whenClause);
        self::assertSame(MergeMatchKind::MATCHED->value, $whenClause->getMatchKind());
        self::assertSame(MergeActionType::DELETE->value, $whenClause->getCommandType());
    }

    public function test_merge_when_matched_do_nothing() : void
    {
        $query = MergeBuilder::create()
            ->into('target')
            ->using('source', 's')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')))
            ->whenMatched()
            ->thenDoNothing();

        $ast = $query->toAst();

        $whenClauses = $ast->getMergeWhenClauses();
        self::assertCount(1, $whenClauses);

        $whenClause = $whenClauses[0]->getMergeWhenClause();
        self::assertNotNull($whenClause);
        self::assertSame(MergeMatchKind::MATCHED->value, $whenClause->getMatchKind());
        self::assertSame(MergeActionType::DO_NOTHING->value, $whenClause->getCommandType());
    }

    public function test_merge_when_matched_returns_correct_instance() : void
    {
        $builder = MergeBuilder::create()
            ->into('users')
            ->using('source', 's')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')));

        $whenMatched = $builder->whenMatched();
        self::assertInstanceOf(MergeWhenMatched::class, $whenMatched);
    }

    public function test_merge_when_matched_update() : void
    {
        $query = MergeBuilder::create()
            ->into('target')
            ->using('source', 's')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')))
            ->whenMatched()
            ->thenUpdate([
                'name' => Column::tableColumn('s', 'name'),
                'value' => Column::tableColumn('s', 'value'),
            ]);

        $ast = $query->toAst();

        $whenClauses = $ast->getMergeWhenClauses();
        self::assertCount(1, $whenClauses);

        $whenClause = $whenClauses[0]->getMergeWhenClause();
        self::assertNotNull($whenClause);
        self::assertSame(MergeMatchKind::MATCHED->value, $whenClause->getMatchKind());
        self::assertSame(MergeActionType::UPDATE->value, $whenClause->getCommandType());

        $targetList = $whenClause->getTargetList();
        self::assertCount(2, $targetList);
    }

    public function test_merge_when_matched_with_condition() : void
    {
        $query = MergeBuilder::create()
            ->into('target')
            ->using('source', 's')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')))
            ->whenMatchedAnd(new Comparison(Column::tableColumn('s', 'status'), ComparisonOperator::EQ, Literal::string('active')))
            ->thenUpdate(['status' => Column::tableColumn('s', 'status')]);

        $ast = $query->toAst();

        $whenClauses = $ast->getMergeWhenClauses();
        self::assertCount(1, $whenClauses);

        $whenClause = $whenClauses[0]->getMergeWhenClause();
        self::assertNotNull($whenClause);
        self::assertTrue($whenClause->hasCondition());
        self::assertNotNull($whenClause->getCondition());
    }

    public function test_merge_when_not_matched_by_source() : void
    {
        $query = MergeBuilder::create()
            ->into('target')
            ->using('source', 's')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')))
            ->whenNotMatchedBySource()
            ->thenDelete();

        $ast = $query->toAst();

        $whenClauses = $ast->getMergeWhenClauses();
        self::assertCount(1, $whenClauses);

        $whenClause = $whenClauses[0]->getMergeWhenClause();
        self::assertNotNull($whenClause);
        self::assertSame(MergeMatchKind::NOT_MATCHED_BY_SOURCE->value, $whenClause->getMatchKind());
        self::assertSame(MergeActionType::DELETE->value, $whenClause->getCommandType());
    }

    public function test_merge_when_not_matched_by_source_with_condition() : void
    {
        $query = MergeBuilder::create()
            ->into('target')
            ->using('source', 's')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')))
            ->whenNotMatchedBySourceAnd(new Comparison(Column::tableColumn('target', 'deleted'), ComparisonOperator::EQ, Literal::bool(false)))
            ->thenUpdate(['deleted' => Literal::bool(true)]);

        $ast = $query->toAst();

        $whenClauses = $ast->getMergeWhenClauses();
        self::assertCount(1, $whenClauses);

        $whenClause = $whenClauses[0]->getMergeWhenClause();
        self::assertNotNull($whenClause);
        self::assertSame(MergeMatchKind::NOT_MATCHED_BY_SOURCE->value, $whenClause->getMatchKind());
        self::assertTrue($whenClause->hasCondition());
    }

    public function test_merge_when_not_matched_do_nothing() : void
    {
        $query = MergeBuilder::create()
            ->into('target')
            ->using('source', 's')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')))
            ->whenNotMatched()
            ->thenDoNothing();

        $ast = $query->toAst();

        $whenClauses = $ast->getMergeWhenClauses();
        self::assertCount(1, $whenClauses);

        $whenClause = $whenClauses[0]->getMergeWhenClause();
        self::assertNotNull($whenClause);
        self::assertSame(MergeMatchKind::NOT_MATCHED_BY_TARGET->value, $whenClause->getMatchKind());
        self::assertSame(MergeActionType::DO_NOTHING->value, $whenClause->getCommandType());
    }

    public function test_merge_when_not_matched_insert() : void
    {
        $query = MergeBuilder::create()
            ->into('target')
            ->using('source', 's')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')))
            ->whenNotMatched()
            ->thenInsert(
                ['id', 'name', 'value'],
                [Column::tableColumn('s', 'id'), Column::tableColumn('s', 'name'), Column::tableColumn('s', 'value')]
            );

        $ast = $query->toAst();

        $whenClauses = $ast->getMergeWhenClauses();
        self::assertCount(1, $whenClauses);

        $whenClause = $whenClauses[0]->getMergeWhenClause();
        self::assertNotNull($whenClause);
        self::assertSame(MergeMatchKind::NOT_MATCHED_BY_TARGET->value, $whenClause->getMatchKind());
        self::assertSame(MergeActionType::INSERT->value, $whenClause->getCommandType());

        $targetList = $whenClause->getTargetList();
        self::assertCount(3, $targetList);

        $values = $whenClause->getValues();
        self::assertCount(3, $values);
    }

    public function test_merge_when_not_matched_insert_values() : void
    {
        $query = MergeBuilder::create()
            ->into('target')
            ->using('source', 's')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')))
            ->whenNotMatched()
            ->thenInsertValues([
                'id' => Column::tableColumn('s', 'id'),
                'name' => Column::tableColumn('s', 'name'),
            ]);

        $ast = $query->toAst();

        $whenClauses = $ast->getMergeWhenClauses();
        self::assertCount(1, $whenClauses);

        $whenClause = $whenClauses[0]->getMergeWhenClause();
        self::assertNotNull($whenClause);
        self::assertSame(MergeActionType::INSERT->value, $whenClause->getCommandType());

        $targetList = $whenClause->getTargetList();
        self::assertCount(2, $targetList);

        $values = $whenClause->getValues();
        self::assertCount(2, $values);
    }

    public function test_merge_when_not_matched_returns_correct_instance() : void
    {
        $builder = MergeBuilder::create()
            ->into('users')
            ->using('source', 's')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')));

        $whenNotMatched = $builder->whenNotMatched();
        self::assertInstanceOf(MergeWhenNotMatched::class, $whenNotMatched);
    }

    public function test_merge_when_not_matched_with_condition() : void
    {
        $query = MergeBuilder::create()
            ->into('target')
            ->using('source', 's')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')))
            ->whenNotMatchedAnd(new Comparison(Column::tableColumn('s', 'type'), ComparisonOperator::EQ, Literal::string('new')))
            ->thenInsertValues(['id' => Column::tableColumn('s', 'id')]);

        $ast = $query->toAst();

        $whenClauses = $ast->getMergeWhenClauses();
        self::assertCount(1, $whenClauses);

        $whenClause = $whenClauses[0]->getMergeWhenClause();
        self::assertNotNull($whenClause);
        self::assertTrue($whenClause->hasCondition());
    }

    public function test_merge_with_conditional_when_matched_to_sql() : void
    {
        self::assertSame(
            'MERGE INTO products p USING price_updates pu ON p.id = pu.product_id WHEN MATCHED AND pu.price > 0 THEN UPDATE SET price = pu.price',
            merge('products', 'p')
                ->using('price_updates', 'pu')
                ->on(eq(col('p.id'), col('pu.product_id')))
                ->whenMatchedAnd(gt(col('pu.price'), literal(0)))
                ->thenUpdate([
                    'price' => col('pu.price'),
                ])
                ->toSql()
        );
    }

    public function test_merge_with_cte_to_sql() : void
    {
        self::assertSame(
            'WITH staged_data AS (SELECT id, name FROM raw_input) MERGE INTO users USING staged_data s ON users.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name',
            with(cte('staged_data', select()
                ->select(col('id'), col('name'))
                ->from(table('raw_input'))))->merge('users')
                ->using('staged_data', 's')
                ->on(eq(col('users.id'), col('s.id')))
                ->whenMatched()
                ->thenUpdate([
                    'name' => col('s.name'),
                ])
                ->toSql()
        );
    }

    public function test_merge_with_delete_to_sql() : void
    {
        self::assertSame(
            'MERGE INTO target_table t USING source_table s ON t.id = s.id WHEN MATCHED THEN DELETE',
            merge('target_table', 't')
                ->using('source_table', 's')
                ->on(eq(col('t.id'), col('s.id')))
                ->whenMatched()
                ->thenDelete()
                ->toSql()
        );
    }

    public function test_merge_with_do_nothing_to_sql() : void
    {
        self::assertSame(
            'MERGE INTO products USING updates u ON products.id = u.id WHEN MATCHED THEN DO NOTHING',
            merge('products')
                ->using('updates', 'u')
                ->on(eq(col('products.id'), col('u.id')))
                ->whenMatched()
                ->thenDoNothing()
                ->toSql()
        );
    }

    public function test_merge_with_insert_to_sql() : void
    {
        self::assertSame(
            'MERGE INTO customers USING new_customers nc ON customers.id = nc.id WHEN NOT MATCHED THEN INSERT (id, name, email) VALUES (nc.id, nc.name, nc.email)',
            merge('customers')
                ->using('new_customers', 'nc')
                ->on(eq(col('customers.id'), col('nc.id')))
                ->whenNotMatched()
                ->thenInsert(
                    ['id', 'name', 'email'],
                    [col('nc.id'), col('nc.name'), col('nc.email')]
                )
                ->toSql()
        );
    }

    public function test_merge_with_insert_values_to_sql() : void
    {
        self::assertSame(
            "MERGE INTO users USING new_users n ON users.id = n.id WHEN NOT MATCHED THEN INSERT (id, name, status) VALUES (n.id, n.name, 'active')",
            merge('users')
                ->using('new_users', 'n')
                ->on(eq(col('users.id'), col('n.id')))
                ->whenNotMatched()
                ->thenInsertValues([
                    'id' => col('n.id'),
                    'name' => col('n.name'),
                    'status' => literal('active'),
                ])
                ->toSql()
        );
    }

    public function test_merge_with_multiple_when_clauses() : void
    {
        $query = MergeBuilder::create()
            ->into('target')
            ->using('source', 's')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')))
            ->whenMatched()
            ->thenUpdate(['value' => Column::tableColumn('s', 'value')])
            ->whenNotMatched()
            ->thenInsertValues(['id' => Column::tableColumn('s', 'id'), 'value' => Column::tableColumn('s', 'value')]);

        $ast = $query->toAst();

        $whenClauses = $ast->getMergeWhenClauses();
        self::assertCount(2, $whenClauses);

        $firstClause = $whenClauses[0]->getMergeWhenClause();
        self::assertNotNull($firstClause);
        self::assertSame(MergeMatchKind::MATCHED->value, $firstClause->getMatchKind());
        self::assertSame(MergeActionType::UPDATE->value, $firstClause->getCommandType());

        $secondClause = $whenClauses[1]->getMergeWhenClause();
        self::assertNotNull($secondClause);
        self::assertSame(MergeMatchKind::NOT_MATCHED_BY_TARGET->value, $secondClause->getMatchKind());
        self::assertSame(MergeActionType::INSERT->value, $secondClause->getCommandType());
    }

    public function test_merge_with_parameters_to_sql() : void
    {
        self::assertSame(
            'MERGE INTO accounts USING transactions t ON accounts.id = t.account_id WHEN MATCHED THEN UPDATE SET balance = $1',
            merge('accounts')
                ->using('transactions', 't')
                ->on(eq(col('accounts.id'), col('t.account_id')))
                ->whenMatched()
                ->thenUpdate([
                    'balance' => param(1),
                ])
                ->toSql()
        );
    }

    public function test_merge_with_source_subquery() : void
    {
        $subquery = SelectBuilder::create()
            ->select(Column::name('id'), Column::name('name'))
            ->from(new Table('raw_data'));

        $query = MergeBuilder::create()
            ->into('target')
            ->using($subquery, 'src')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')))
            ->whenMatched()
            ->thenDoNothing();

        $ast = $query->toAst();

        $sourceRelation = $ast->getSourceRelation();
        self::assertNotNull($sourceRelation);
        self::assertTrue($sourceRelation->hasRangeSubselect());

        $rangeSubselect = $sourceRelation->getRangeSubselect();
        self::assertNotNull($rangeSubselect);
        self::assertTrue($rangeSubselect->hasSubquery());

        $alias = $rangeSubselect->getAlias();
        self::assertNotNull($alias);
        self::assertSame('src', $alias->getAliasname());
    }

    public function test_merge_with_source_table() : void
    {
        $query = MergeBuilder::create()
            ->into('target')
            ->using('source_table', 'src')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')))
            ->whenMatched()
            ->thenDoNothing();

        $ast = $query->toAst();

        $sourceRelation = $ast->getSourceRelation();
        self::assertNotNull($sourceRelation);
        self::assertTrue($sourceRelation->hasRangeVar());

        $rangeVar = $sourceRelation->getRangeVar();
        self::assertNotNull($rangeVar);
        self::assertSame('source_table', $rangeVar->getRelname());

        $alias = $rangeVar->getAlias();
        self::assertNotNull($alias);
        self::assertSame('src', $alias->getAliasname());
    }

    public function test_merge_with_table_alias() : void
    {
        $query = MergeBuilder::create()
            ->into('users', 'u')
            ->using('source', 's')
            ->on(new Comparison(Column::tableColumn('u', 'id'), ComparisonOperator::EQ, Column::tableColumn('s', 'id')))
            ->whenMatched()
            ->thenDoNothing();

        $ast = $query->toAst();

        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('users', $relation->getRelname());

        $alias = $relation->getAlias();
        self::assertNotNull($alias);
        self::assertSame('u', $alias->getAliasname());
    }

    public function test_merge_with_with_clause() : void
    {
        $selectStmt = new SelectStmt();
        $selectNode = new Node();
        $selectNode->setSelectStmt($selectStmt);

        $cte = new CTE('staged_data', $selectNode);
        $withClause = new WithClause([$cte]);

        $query = MergeBuilder::with($withClause)
            ->into('target')
            ->using('staged_data', 's')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')))
            ->whenMatched()
            ->thenUpdate(['name' => Column::tableColumn('s', 'name')]);

        $ast = $query->toAst();

        self::assertTrue($ast->hasWithClause());
        $withClauseProto = $ast->getWithClause();
        self::assertNotNull($withClauseProto);

        $ctes = $withClauseProto->getCtes();
        self::assertNotNull($ctes);
        self::assertCount(1, $ctes);

        $firstCte = $ctes[0]->getCommonTableExpr();
        self::assertNotNull($firstCte);
        self::assertSame('staged_data', $firstCte->getCtename());
    }

    public function test_merge_without_table_alias() : void
    {
        $query = MergeBuilder::create()
            ->into('users')
            ->using('source', 's')
            ->on(new Comparison(Column::tableColumn('users', 'id'), ComparisonOperator::EQ, Column::tableColumn('s', 'id')))
            ->whenMatched()
            ->thenDoNothing();

        $ast = $query->toAst();

        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('users', $relation->getRelname());
        self::assertFalse($relation->hasAlias());
    }

    public function test_to_ast_without_join_condition_throws_exception() : void
    {
        $this->expectException(InvalidExpressionException::class);

        $builder = MergeBuilder::create()
            ->into('users')
            ->using('source', 's');

        \assert($builder instanceof MergeFinalStep);
        $builder->toAst();
    }

    public function test_to_ast_without_source_alias_throws_exception() : void
    {
        $this->expectException(InvalidExpressionException::class);

        MergeBuilder::create()
            ->into('users')
            ->using('source', '')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')))
            ->whenMatched()
            ->thenDoNothing()
            ->toAst();
    }

    public function test_to_ast_without_source_throws_exception() : void
    {
        $this->expectException(InvalidExpressionException::class);

        $builder = MergeBuilder::create()->into('users');
        \assert($builder instanceof MergeFinalStep);
        $builder->toAst();
    }

    public function test_to_ast_without_table_throws_exception() : void
    {
        $this->expectException(InvalidExpressionException::class);

        $builder = MergeBuilder::create();
        \assert($builder instanceof MergeFinalStep);
        $builder->toAst();
    }

    public function test_to_ast_without_when_clauses_throws_exception() : void
    {
        $this->expectException(InvalidExpressionException::class);

        $builder = MergeBuilder::create()
            ->into('users')
            ->using('source', 's')
            ->on(new Comparison(Column::name('id'), ComparisonOperator::EQ, Column::name('id')));

        $builder->toAst();
    }

    private function deparse(MergeStmt $mergeStmt) : string
    {
        $parser = new Parser();
        $node = new Node();
        $node->setMergeStmt($mergeStmt);
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        return (new ParsedQuery($parseResult))->deparse();
    }
}
