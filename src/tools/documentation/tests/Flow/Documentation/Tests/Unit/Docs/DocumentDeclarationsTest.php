<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Unit\Docs;

use Flow\Documentation\Docs\DocumentDeclarations;
use Flow\Documentation\Docs\Fence;
use Flow\Documentation\Docs\FenceInfo;
use PHPUnit\Framework\TestCase;

final class DocumentDeclarationsTest extends TestCase
{
    public function test_a_name_declared_in_one_fence_counts_for_the_whole_file(): void
    {
        $declarations = new DocumentDeclarations([
            new Fence('a.md', 1, FenceInfo::parse('php'), 'function helper(): void {}'),
            new Fence('a.md', 9, FenceInfo::parse('php'), 'helper();'),
        ]);

        static::assertTrue($declarations->declares('helper'));
    }

    public function test_declaration_lookup_ignores_case(): void
    {
        static::assertTrue((new DocumentDeclarations([new Fence(
            'a.md',
            1,
            FenceInfo::parse('php'),
            'function Helper(): void {}',
        )]))->declares('helper'));
    }

    public function test_a_name_nothing_declares_is_not_declared(): void
    {
        static::assertFalse((new DocumentDeclarations([]))->declares('helper'));
    }

    public function test_a_method_declaration_counts_too(): void
    {
        static::assertTrue((new DocumentDeclarations([new Fence(
            'a.md',
            1,
            FenceInfo::parse('php'),
            'class A { public function run(): void {} }',
        )]))->declares('run'));
    }
}
