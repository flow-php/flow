<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\FuncCall;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\Expression\FunctionCall;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\func;

final class FunctionCallTest extends TestCase
{
    public function test_converts_function_with_schema_to_ast(): void
    {
        $func = new FunctionCall(['pg_catalog', 'sum'], []);

        $node = $func->toAst();
        $funcCall = $node->getFuncCall();
        static::assertNotNull($funcCall);
        $funcNameNodes = $funcCall->getFuncname();

        static::assertCount(2, $funcNameNodes);
        $firstString = $funcNameNodes[0]->getString();
        static::assertNotNull($firstString);
        static::assertSame('pg_catalog', $firstString->getSval());
        $secondString = $funcNameNodes[1]->getString();
        static::assertNotNull($secondString);
        static::assertSame('sum', $secondString->getSval());
    }

    public function test_converts_to_ast(): void
    {
        $arg = Literal::int(42);
        $func = new FunctionCall(['test_func'], [$arg]);

        $node = $func->toAst();

        $funcCall = $node->getFuncCall();
        static::assertNotNull($funcCall);

        $funcNameNodes = $funcCall->getFuncname();

        static::assertCount(1, $funcNameNodes);
        $firstString = $funcNameNodes[0]->getString();
        static::assertNotNull($firstString);
        static::assertSame('test_func', $firstString->getSval());

        $argNodes = $funcCall->getArgs();
        static::assertCount(1, $argNodes);
    }

    public function test_creates_aliased_expression(): void
    {
        $func = new FunctionCall(['test'], []);
        $aliased = $func->as('my_alias');

        static::assertSame('my_alias', $aliased->getAlias());
        static::assertSame($func, $aliased->getExpression());
    }

    public function test_creates_function_call_with_arguments(): void
    {
        $arg1 = Literal::int(1);
        $arg2 = Literal::string('test');

        $func = new FunctionCall(['my_func'], [$arg1, $arg2]);

        static::assertCount(2, $func->getArgs());
    }

    public function test_creates_function_call_with_schema(): void
    {
        $func = new FunctionCall(['pg_catalog', 'count'], []);

        static::assertSame(['pg_catalog', 'count'], $func->getFuncName());
    }

    public function test_creates_simple_function_call(): void
    {
        $func = new FunctionCall(['my_func'], []);

        static::assertSame(['my_func'], $func->getFuncName());
        static::assertSame([], $func->getArgs());
    }

    public function test_recreates_from_ast(): void
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

        static::assertSame(['my_func'], $func->getFuncName());
        static::assertSame([], $func->getArgs());
    }

    public function test_round_trip_conversion(): void
    {
        $arg1 = Literal::int(10);
        $arg2 = Literal::string('value');
        $func = new FunctionCall(['pg_catalog', 'concat'], [$arg1, $arg2]);

        $node = $func->toAst();
        $restored = FunctionCall::fromAst($node);

        static::assertSame($func->getFuncName(), $restored->getFuncName());
        static::assertCount(2, $restored->getArgs());
    }

    public function test_throws_exception_for_empty_function_name(): void
    {
        $this->expectException(InvalidExpressionException::class);

        (new ReflectionClass(FunctionCall::class))->newInstance([], []);
    }

    public function test_with_args_creates_new_instance(): void
    {
        $func = new FunctionCall(['test'], []);
        $arg = Literal::int(1);

        $newFunc = $func->withArgs($arg);

        static::assertNotSame($func, $newFunc);
        static::assertSame([], $func->getArgs());
        static::assertCount(1, $newFunc->getArgs());
    }

    #[TestWith(['greatest'])]
    #[TestWith(['least'])]
    #[TestWith(['coalesce'])]
    #[TestWith(['nullif'])]
    #[TestWith(['current_timestamp'])]
    #[TestWith(['current_date'])]
    #[TestWith(['current_time'])]
    #[TestWith(['GREATEST'])]
    public function test_func_rejects_keyword_construct(string $name): void
    {
        $this->expectException(InvalidExpressionException::class);

        func($name, [col('a'), col('b')]);
    }

    public function test_func_allows_regular_function_name(): void
    {
        static::assertSame(['to_char'], func('to_char', [col('a'), col('b')])->getFuncName());
    }

    public function test_function_call_constructor_still_allows_keyword_name_for_round_trip(): void
    {
        static::assertSame(['greatest'], (new FunctionCall(['greatest'], [col('a')]))->getFuncName());
    }
}
