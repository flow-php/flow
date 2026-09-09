<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Unit\Docs;

use Flow\Documentation\Docs\FenceSymbols;
use Flow\Documentation\Tests\Context\FenceContext;
use PHPUnit\Framework\TestCase;

final class FenceSymbolsTest extends TestCase
{
    private FenceContext $fences;

    protected function setUp(): void
    {
        $this->fences = new FenceContext();
    }

    public function test_a_fragment_without_a_php_tag_still_tokenizes(): void
    {
        static::assertTrue((new FenceSymbols())->tokenizes($this->fences->php('echo from_csv("a.csv");')));
    }

    public function test_a_snippet_missing_its_terminator_does_not_tokenize(): void
    {
        static::assertFalse((new FenceSymbols())->tokenizes($this->fences->php(
            'data_frame()->read(from_csv("a.csv"))',
        )));
    }

    public function test_it_reports_a_function_this_project_does_not_define(): void
    {
        static::assertSame(['to_database'], $this->fences->unresolvedFunctions('to_database($connection, "orders");'));
    }

    public function test_it_accepts_a_function_this_project_defines(): void
    {
        static::assertSame([], $this->fences->unresolvedFunctions('from_csv("a.csv");'));
    }

    public function test_it_accepts_a_php_internal_function(): void
    {
        static::assertSame([], $this->fences->unresolvedFunctions('strlen("a");'));
    }

    public function test_it_ignores_a_name_that_only_appears_inside_a_string(): void
    {
        static::assertSame([], $this->fences->unresolvedFunctions('$sql = "SELECT count(*) FROM to_database";'));
    }

    public function test_it_does_not_judge_method_calls(): void
    {
        static::assertSame([], $this->fences->unresolvedFunctions('$a->thereIsNoSuchMethodAnywhere();'));
    }

    public function test_it_skips_a_function_the_page_declares_itself(): void
    {
        static::assertSame(
            [],
            $this->fences->unresolvedFunctions('processEvent($row);', 'function processEvent(array $row): void {}'),
        );
    }

    public function test_it_skips_a_function_declaration_rather_than_treating_it_as_a_call(): void
    {
        static::assertSame([], $this->fences->unresolvedFunctions('function neverDefinedAnywhere(): void {}'));
    }

    public function test_an_unparseable_fence_reports_nothing(): void
    {
        static::assertSame([], $this->fences->unresolvedFunctions('data_frame()->read(from_csv("a.csv"))'));
    }
}
