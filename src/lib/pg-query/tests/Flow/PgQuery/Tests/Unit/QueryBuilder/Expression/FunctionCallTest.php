<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Expression;

use Flow\PgQuery\Protobuf\AST\{FuncCall, Node, PBString};
use Flow\PgQuery\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PgQuery\QueryBuilder\Expression\{FunctionCall, Literal};
use PHPUnit\Framework\TestCase;

final class FunctionCallTest extends TestCase
{
    public function test_converts_function_with_schema_to_ast() : void
    {
        $func = new FunctionCall(['pg_catalog', 'sum'], []);

        $node = $func->toAst();
        $funcCall = $node->getFuncCall();
        self::assertNotNull($funcCall);
        $funcNameNodes = $funcCall->getFuncname();

        self::assertCount(2, $funcNameNodes);
        $firstString = $funcNameNodes[0]->getString();
        self::assertNotNull($firstString);
        self::assertSame('pg_catalog', $firstString->getSval());
        $secondString = $funcNameNodes[1]->getString();
        self::assertNotNull($secondString);
        self::assertSame('sum', $secondString->getSval());
    }

    public function test_converts_to_ast() : void
    {
        $arg = Literal::int(42);
        $func = new FunctionCall(['test_func'], [$arg]);

        $node = $func->toAst();

        $funcCall = $node->getFuncCall();
        self::assertNotNull($funcCall);

        $funcNameNodes = $funcCall->getFuncname();

        self::assertCount(1, $funcNameNodes);
        $firstString = $funcNameNodes[0]->getString();
        self::assertNotNull($firstString);
        self::assertSame('test_func', $firstString->getSval());

        $argNodes = $funcCall->getArgs();
        self::assertCount(1, $argNodes);
    }

    public function test_creates_aliased_expression() : void
    {
        $func = new FunctionCall(['test'], []);
        $aliased = $func->as('my_alias');

        self::assertSame('my_alias', $aliased->getAlias());
        self::assertSame($func, $aliased->getExpression());
    }

    public function test_creates_function_call_with_arguments() : void
    {
        $arg1 = Literal::int(1);
        $arg2 = Literal::string('test');

        $func = new FunctionCall(['my_func'], [$arg1, $arg2]);

        self::assertCount(2, $func->getArgs());
    }

    public function test_creates_function_call_with_schema() : void
    {
        $func = new FunctionCall(['pg_catalog', 'count'], []);

        self::assertSame(['pg_catalog', 'count'], $func->getFuncName());
    }

    public function test_creates_simple_function_call() : void
    {
        $func = new FunctionCall(['my_func'], []);

        self::assertSame(['my_func'], $func->getFuncName());
        self::assertSame([], $func->getArgs());
    }

    public function test_recreates_from_ast() : void
    {
        $stringNode = new PBString();
        $stringNode->setSval('my_func');

        $nameNode = new Node();
        $nameNode->setString($stringNode);

        $funcCall = new FuncCall();
        $funcCall->setFuncname([$nameNode]);
        $funcCall->setArgs([]);

        $node = new Node();
        $node->setFuncCall($funcCall);

        $func = FunctionCall::fromAst($node);

        self::assertSame(['my_func'], $func->getFuncName());
        self::assertSame([], $func->getArgs());
    }

    public function test_round_trip_conversion() : void
    {
        $arg1 = Literal::int(10);
        $arg2 = Literal::string('value');
        $func = new FunctionCall(['pg_catalog', 'concat'], [$arg1, $arg2]);

        $node = $func->toAst();
        $restored = FunctionCall::fromAst($node);

        self::assertSame($func->getFuncName(), $restored->getFuncName());
        self::assertCount(2, $restored->getArgs());
    }

    public function test_throws_exception_for_empty_function_name() : void
    {
        $this->expectException(InvalidExpressionException::class);

        /** @phpstan-ignore argument.type (intentionally testing exception) */
        new FunctionCall([], []);
    }

    public function test_with_args_creates_new_instance() : void
    {
        $func = new FunctionCall(['test'], []);
        $arg = Literal::int(1);

        $newFunc = $func->withArgs($arg);

        self::assertNotSame($func, $newFunc);
        self::assertSame([], $func->getArgs());
        self::assertCount(1, $newFunc->getArgs());
    }
}
