<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\MergeSupportFunc;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\MergeActionFunction;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\merge_action;

final class MergeActionFunctionTest extends TestCase
{
    public function test_from_ast_of_another_node_throws(): void
    {
        $this->expectException(InvalidAstException::class);

        MergeActionFunction::fromAst(literal(1)->toAst());
    }

    public function test_from_ast_reads_merge_support_func_node(): void
    {
        static::assertEquals(
            merge_action(),
            MergeActionFunction::fromAst((new Node())->setMergeSupportFunc(new MergeSupportFunc())),
        );
    }

    public function test_to_ast_gives_merge_support_func_node(): void
    {
        static::assertNotNull(merge_action()->toAst()->getMergeSupportFunc());
    }
}
