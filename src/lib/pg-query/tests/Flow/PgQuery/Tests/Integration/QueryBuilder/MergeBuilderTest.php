<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{
    col,
    cte,
    eq,
    gt,
    literal,
    merge,
    param,
    select,
    table,
    with
};

use Flow\PgQuery\{ParsedQuery, Parser};
use Flow\PgQuery\Protobuf\AST\{MergeStmt, Node, RawStmt};
use Flow\PgQuery\QueryBuilder\Merge\MergeFinalStep;

final class MergeBuilderTest extends PGQueryTestCase
{
    public function test_merge_simple_update() : void
    {
        $query = merge('target')
            ->using('source', 's')
            ->on(eq(col('target.id'), col('s.id')))
            ->whenMatched()
            ->thenUpdate([
                'value' => col('s.value'),
            ]);

        $this->assertMergeQueryEquals(
            $query,
            'MERGE INTO target USING source s ON target.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value'
        );
    }

    public function test_merge_using_subquery() : void
    {
        $sourceQuery = select()
            ->select(col('id'), col('name'), col('email'))
            ->from(table('staged_data'));

        $query = merge('users')
            ->using($sourceQuery, 'src')
            ->on(eq(col('users.id'), col('src.id')))
            ->whenMatched()
            ->thenUpdate([
                'name' => col('src.name'),
                'email' => col('src.email'),
            ]);

        $this->assertMergeQueryEquals(
            $query,
            'MERGE INTO users USING (SELECT id, name, email FROM staged_data) src ON users.id = src.id WHEN MATCHED THEN UPDATE SET name = src.name, email = src.email'
        );
    }

    public function test_merge_when_not_matched_do_nothing() : void
    {
        $query = merge('target')
            ->using('source', 's')
            ->on(eq(col('target.id'), col('s.id')))
            ->whenNotMatched()
            ->thenDoNothing();

        $this->assertMergeQueryEquals(
            $query,
            'MERGE INTO target USING source s ON target.id = s.id WHEN NOT MATCHED THEN DO NOTHING'
        );
    }

    public function test_merge_with_conditional_when_matched() : void
    {
        $query = merge('products', 'p')
            ->using('price_updates', 'pu')
            ->on(eq(col('p.id'), col('pu.product_id')))
            ->whenMatchedAnd(gt(col('pu.price'), literal(0)))
            ->thenUpdate([
                'price' => col('pu.price'),
            ]);

        $this->assertMergeQueryEquals(
            $query,
            'MERGE INTO products p USING price_updates pu ON p.id = pu.product_id WHEN MATCHED AND pu.price > 0 THEN UPDATE SET price = pu.price'
        );
    }

    public function test_merge_with_cte() : void
    {
        $stagedData = select()
            ->select(col('id'), col('name'))
            ->from(table('raw_input'));

        $query = with(cte('staged_data', $stagedData))->merge('users')
            ->using('staged_data', 's')
            ->on(eq(col('users.id'), col('s.id')))
            ->whenMatched()
            ->thenUpdate([
                'name' => col('s.name'),
            ]);

        $this->assertMergeQueryEquals(
            $query,
            'WITH staged_data AS (SELECT id, name FROM raw_input) MERGE INTO users USING staged_data s ON users.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name'
        );
    }

    public function test_merge_with_delete() : void
    {
        $query = merge('target_table', 't')
            ->using('source_table', 's')
            ->on(eq(col('t.id'), col('s.id')))
            ->whenMatched()
            ->thenDelete();

        $this->assertMergeQueryEquals(
            $query,
            'MERGE INTO target_table t USING source_table s ON t.id = s.id WHEN MATCHED THEN DELETE'
        );
    }

    public function test_merge_with_do_nothing() : void
    {
        $query = merge('products')
            ->using('updates', 'u')
            ->on(eq(col('products.id'), col('u.id')))
            ->whenMatched()
            ->thenDoNothing();

        $this->assertMergeQueryEquals(
            $query,
            'MERGE INTO products USING updates u ON products.id = u.id WHEN MATCHED THEN DO NOTHING'
        );
    }

    public function test_merge_with_insert() : void
    {
        $query = merge('customers')
            ->using('new_customers', 'nc')
            ->on(eq(col('customers.id'), col('nc.id')))
            ->whenNotMatched()
            ->thenInsert(
                ['id', 'name', 'email'],
                [col('nc.id'), col('nc.name'), col('nc.email')]
            );

        $this->assertMergeQueryEquals(
            $query,
            'MERGE INTO customers USING new_customers nc ON customers.id = nc.id WHEN NOT MATCHED THEN INSERT (id, name, email) VALUES (nc.id, nc.name, nc.email)'
        );
    }

    public function test_merge_with_insert_values() : void
    {
        $query = merge('users')
            ->using('new_users', 'n')
            ->on(eq(col('users.id'), col('n.id')))
            ->whenNotMatched()
            ->thenInsertValues([
                'id' => col('n.id'),
                'name' => col('n.name'),
                'status' => literal('active'),
            ]);

        $this->assertMergeQueryEquals(
            $query,
            "MERGE INTO users USING new_users n ON users.id = n.id WHEN NOT MATCHED THEN INSERT (id, name, status) VALUES (n.id, n.name, 'active')"
        );
    }

    public function test_merge_with_multiple_when_clauses() : void
    {
        $query = merge('inventory', 'i')
            ->using('updates', 'u')
            ->on(eq(col('i.product_id'), col('u.product_id')))
            ->whenMatched()
            ->thenUpdate([
                'quantity' => col('u.quantity'),
                'updated_at' => col('u.updated_at'),
            ])
            ->whenNotMatched()
            ->thenInsert(
                ['product_id', 'quantity'],
                [col('u.product_id'), col('u.quantity')]
            );

        $this->assertMergeQueryEquals(
            $query,
            'MERGE INTO inventory i USING updates u ON i.product_id = u.product_id WHEN MATCHED THEN UPDATE SET quantity = u.quantity, updated_at = u.updated_at WHEN NOT MATCHED THEN INSERT (product_id, quantity) VALUES (u.product_id, u.quantity)'
        );
    }

    public function test_merge_with_parameters() : void
    {
        $query = merge('accounts')
            ->using('transactions', 't')
            ->on(eq(col('accounts.id'), col('t.account_id')))
            ->whenMatched()
            ->thenUpdate([
                'balance' => param(1),
            ]);

        $this->assertMergeQueryEquals(
            $query,
            'MERGE INTO accounts USING transactions t ON accounts.id = t.account_id WHEN MATCHED THEN UPDATE SET balance = $1'
        );
    }

    protected function assertMergeQueryEquals(MergeFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseMergeStmt($builder->toAst());

        self::assertSame($expectedSql, $sql);
    }

    protected function deparseMergeStmt(MergeStmt $mergeStmt) : string
    {
        $node = new Node();
        $node->setMergeStmt($mergeStmt);

        $parser = new Parser();
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        return (new ParsedQuery($parseResult))->deparse();
    }
}
