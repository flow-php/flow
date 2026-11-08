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

    public function test_it_reports_a_function_imported_from_a_namespace_that_does_not_define_it(): void
    {
        static::assertSame(
            ['Flow\ETL\DSL\from_csv', 'Flow\ETL\DSL\to_csv'],
            $this->fences->unresolvedFunctions(
                'use function Flow\ETL\DSL\{config_builder, from_csv, to_csv}; from_csv("a.csv"); to_csv("b.csv");',
            ),
        );
    }

    public function test_it_reports_a_single_import_from_the_wrong_namespace(): void
    {
        static::assertSame(
            ['Flow\ETL\DSL\from_csv'],
            $this->fences->unresolvedFunctions('use function Flow\ETL\DSL\from_csv; from_csv("a.csv");'),
        );
    }

    public function test_it_accepts_functions_imported_from_the_namespace_that_defines_them(): void
    {
        static::assertSame([], $this->fences->unresolvedFunctions(
            'use function Flow\ETL\Adapter\CSV\{from_csv, to_csv as csv}; use function Flow\ETL\DSL\ref; '
            . 'from_csv("a.csv"); csv("b.csv"); ref("id");',
        ));
    }

    public function test_a_closure_use_and_a_class_import_are_not_function_imports(): void
    {
        static::assertSame(
            [],
            $this->fences->unresolvedFunctions(
                'use Flow\ETL\Nope\Missing; $f = function () use ($a) { return strlen($a); };',
            ),
        );
    }
}
