<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Constraint;

use function Flow\PostgreSql\DSL\{col, eq, func, literal};
use Flow\PostgreSql\Protobuf\AST\{ConstrType, Constraint, IndexElem, PBList, PBString};
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\ExcludeConstraint;
use PHPUnit\Framework\TestCase;

final class ExcludeConstraintTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_deferrable_initially_deferred_emits_flags_on_ast() : void
    {
        $ast = ExcludeConstraint::create('btree')
            ->element(col('room_id'), '=')
            ->deferrable(true)
            ->toAst();

        self::assertTrue($ast->getDeferrable());
        self::assertTrue($ast->getInitdeferred());
    }

    public function test_deferrable_initially_immediate_does_not_set_initdeferred() : void
    {
        $ast = ExcludeConstraint::create('btree')
            ->element(col('room_id'), '=')
            ->deferrable()
            ->toAst();

        self::assertTrue($ast->getDeferrable());
        self::assertFalse($ast->getInitdeferred());
    }

    public function test_element_expression_uses_index_elem_expr_field() : void
    {
        $ast = ExcludeConstraint::create('gist')
            ->element(func('tsrange', [col('start'), col('finish')]), '&&')
            ->toAst();

        $exclusions = $ast->getExclusions();
        $items = \iterator_to_array($exclusions[0]->getList()->getItems());

        $indexElem = $items[0]->getIndexElem();
        self::assertSame('', $indexElem->getName());
        self::assertNotNull($indexElem->getExpr());
    }

    public function test_exclude_constraint_with_btree() : void
    {
        $constraint = ExcludeConstraint::create('btree')
            ->element(col('id'), '=');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame(ConstrType::CONSTR_EXCLUSION, $ast->getContype());
        self::assertSame('btree', $ast->getAccessMethod());
    }

    public function test_exclude_constraint_with_multiple_elements() : void
    {
        $constraint = ExcludeConstraint::create('gist')
            ->element(col('room_id'), '=')
            ->element(col('during'), '&&');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame(ConstrType::CONSTR_EXCLUSION, $ast->getContype());
        self::assertCount(2, $ast->getExclusions());
    }

    public function test_exclude_constraint_with_name() : void
    {
        $constraint = ExcludeConstraint::create()
            ->element(col('room_id'), '=')
            ->name('exc_room_booking');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame('exc_room_booking', $ast->getConname());
    }

    public function test_exclude_constraint_with_where_clause() : void
    {
        $constraint = ExcludeConstraint::create()
            ->element(col('room_id'), '=')
            ->where(eq(col('active'), literal(true)));

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertTrue($ast->hasWhereClause());
    }

    public function test_exclude_with_all_options() : void
    {
        $constraint = ExcludeConstraint::create('gist')
            ->name('exc_booking')
            ->element(col('room_id'), '=')
            ->element(col('period'), '&&')
            ->where(eq(col('cancelled'), literal(false)));

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame('exc_booking', $ast->getConname());
        self::assertSame('gist', $ast->getAccessMethod());
        self::assertCount(2, $ast->getExclusions());
        self::assertTrue($ast->hasWhereClause());
    }

    public function test_exclusions_are_list_of_pairs_of_index_elem_and_operator_list() : void
    {
        $ast = ExcludeConstraint::create('btree')
            ->element(col('room_id'), '=')
            ->toAst();

        $exclusions = $ast->getExclusions();

        self::assertCount(1, $exclusions);

        $pairList = $exclusions[0]->getList();
        self::assertInstanceOf(PBList::class, $pairList);

        $items = \iterator_to_array($pairList->getItems());
        self::assertCount(2, $items);

        $indexElem = $items[0]->getIndexElem();
        self::assertInstanceOf(IndexElem::class, $indexElem);
        self::assertSame('room_id', $indexElem->getName());

        $operatorList = $items[1]->getList();
        self::assertInstanceOf(PBList::class, $operatorList);
        $operatorItems = \iterator_to_array($operatorList->getItems());
        self::assertCount(1, $operatorItems);
        $operatorString = $operatorItems[0]->getString();
        self::assertInstanceOf(PBString::class, $operatorString);
        self::assertSame('=', $operatorString->getSval());
    }

    public function test_immutability() : void
    {
        $original = ExcludeConstraint::create();
        $withName = $original->name('exc_test');

        self::assertNotSame($original, $withName);
        self::assertSame('', $original->toAst()->getConname());
        self::assertSame('exc_test', $withName->toAst()->getConname());
    }

    public function test_not_deferrable_by_default() : void
    {
        $ast = ExcludeConstraint::create('btree')
            ->element(col('room_id'), '=')
            ->toAst();

        self::assertFalse($ast->getDeferrable());
        self::assertFalse($ast->getInitdeferred());
    }

    public function test_simple_exclude_constraint() : void
    {
        $constraint = ExcludeConstraint::create()
            ->element(col('room_id'), '=');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame(ConstrType::CONSTR_EXCLUSION, $ast->getContype());
        self::assertSame('gist', $ast->getAccessMethod());
        self::assertCount(1, $ast->getExclusions());
    }
}
