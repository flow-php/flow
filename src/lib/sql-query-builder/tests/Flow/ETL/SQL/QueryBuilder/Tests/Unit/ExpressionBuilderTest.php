<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder\Tests\Unit;

use Flow\ETL\SQL\QueryBuilder\ExpressionBuilder;
use Flow\ETL\SQL\QueryBuilder\Platform\GenericPlatform;
use Flow\ETL\SQL\QueryBuilder\Platform\MySQLPlatform;
use PHPUnit\Framework\TestCase;

class ExpressionBuilderTest extends TestCase
{
    private ExpressionBuilder $expr;

    protected function setUp(): void
    {
        $this->expr = new ExpressionBuilder(new GenericPlatform());
    }

    public function test_comparison_expressions(): void
    {
        self::assertEquals('"column" = :value', $this->expr->eq('column', ':value'));
        self::assertEquals('"column" != :value', $this->expr->neq('column', ':value'));
        self::assertEquals('"column" < :value', $this->expr->lt('column', ':value'));
        self::assertEquals('"column" <= :value', $this->expr->lte('column', ':value'));
        self::assertEquals('"column" > :value', $this->expr->gt('column', ':value'));
        self::assertEquals('"column" >= :value', $this->expr->gte('column', ':value'));
    }

    public function test_null_expressions(): void
    {
        self::assertEquals('"column" IS NULL', $this->expr->isNull('column'));
        self::assertEquals('"column" IS NOT NULL', $this->expr->isNotNull('column'));
    }

    public function test_like_expressions(): void
    {
        self::assertEquals('"name" LIKE :pattern', $this->expr->like('name', ':pattern'));
        self::assertEquals('"name" NOT LIKE :pattern', $this->expr->notLike('name', ':pattern'));
    }

    public function test_between_expression(): void
    {
        self::assertEquals('"age" BETWEEN :min AND :max', $this->expr->between('age', ':min', ':max'));
    }

    public function test_in_expressions(): void
    {
        // With parameter placeholder
        self::assertEquals('"id" IN (:ids)', $this->expr->in('id', ':ids'));
        
        // With array values
        self::assertEquals('"status" IN (\'active\', \'pending\')', $this->expr->in('status', ['active', 'pending']));
        
        // Empty array
        self::assertEquals('1 = 0', $this->expr->in('id', []));
    }

    public function test_not_in_expressions(): void
    {
        // With parameter placeholder
        self::assertEquals('"id" NOT IN (:ids)', $this->expr->notIn('id', ':ids'));
        
        // With array values
        self::assertEquals('"status" NOT IN (\'inactive\', \'deleted\')', $this->expr->notIn('status', ['inactive', 'deleted']));
        
        // Empty array
        self::assertEquals('1 = 1', $this->expr->notIn('id', []));
    }

    public function test_logical_expressions(): void
    {
        $cond1 = $this->expr->eq('status', ':status');
        $cond2 = $this->expr->gt('age', ':age');
        $cond3 = $this->expr->isNotNull('email');
        
        self::assertEquals(
            '("status" = :status AND "age" > :age AND "email" IS NOT NULL)',
            $this->expr->andX($cond1, $cond2, $cond3)
        );
        
        self::assertEquals(
            '("status" = :status OR "age" > :age OR "email" IS NOT NULL)',
            $this->expr->orX($cond1, $cond2, $cond3)
        );
        
        self::assertEquals(
            'NOT ("status" = :status)',
            $this->expr->not($cond1)
        );
    }

    public function test_empty_logical_expressions(): void
    {
        // Empty AND returns always true
        self::assertEquals('1 = 1', $this->expr->andX());
        
        // Empty OR returns always false
        self::assertEquals('1 = 0', $this->expr->orX());
    }

    public function test_quote_identifier(): void
    {
        self::assertEquals('"table_name"', $this->expr->quoteIdentifier('table_name'));
        self::assertEquals('"table"."column"', $this->expr->quoteIdentifier('table.column'));
        
        // Already quoted
        self::assertEquals('"already_quoted"', $this->expr->quoteIdentifier('"already_quoted"'));
    }

    public function test_quote_identifier_mysql(): void
    {
        $mysqlExpr = new ExpressionBuilder(new MySQLPlatform());
        
        self::assertEquals('`table_name`', $mysqlExpr->quoteIdentifier('table_name'));
        self::assertEquals('`table`.`column`', $mysqlExpr->quoteIdentifier('table.column'));
    }

    public function test_literal_values(): void
    {
        self::assertEquals('NULL', $this->expr->literal(null));
        self::assertEquals('TRUE', $this->expr->literal(true));
        self::assertEquals('FALSE', $this->expr->literal(false));
        self::assertEquals('42', $this->expr->literal(42));
        self::assertEquals('3.14', $this->expr->literal(3.14));
        self::assertEquals("'hello world'", $this->expr->literal('hello world'));
        
        // String escaping
        self::assertEquals("'it''s escaped'", $this->expr->literal("it's escaped"));
    }

    public function test_literal_with_mysql_platform(): void
    {
        $mysqlExpr = new ExpressionBuilder(new MySQLPlatform());
        
        // MySQL uses 1/0 for booleans
        self::assertEquals('1', $mysqlExpr->literal(true));
        self::assertEquals('0', $mysqlExpr->literal(false));
    }

    public function test_complex_expression(): void
    {
        $expr = $this->expr->andX(
            $this->expr->eq('status', ':status'),
            $this->expr->orX(
                $this->expr->gte('age', ':min_age'),
                $this->expr->in('role', ['admin', 'moderator'])
            ),
            $this->expr->not($this->expr->isNull('email'))
        );
        
        $expected = '("status" = :status AND ("age" >= :min_age OR "role" IN (\'admin\', \'moderator\')) AND NOT ("email" IS NULL))';
        
        self::assertEquals($expected, $expr);
    }

    public function test_literal_throws_on_array(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot convert array to SQL literal');
        
        $this->expr->literal(['array', 'value']);
    }

    public function test_literal_throws_on_object_without_toString(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot convert object to SQL literal');
        
        $this->expr->literal(new \stdClass());
    }
}