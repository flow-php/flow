<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Parser;

use Flow\PostgreSql\Parser\ExpressionParser;
use Flow\PostgreSql\Tests\Mother\ParsedExcludeDefinitionMother;
use PHPUnit\Framework\TestCase;

use function extension_loaded;

final class ParsedExcludeDefinitionTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_normalized_strips_casts_from_elements_and_predicate(): void
    {
        $normalized = ParsedExcludeDefinitionMother::with(elements: [[
            'expression' => 'lower((name)::text)',
            'operator' => '=',
        ]], predicate: "((status)::text = 'active'::text)")->normalized(new ExpressionParser());

        static::assertSame([['expression' => 'lower(name)', 'operator' => '=']], $normalized->elements);
        static::assertSame("status = 'active'", $normalized->predicate);
    }

    public function test_equal_when_all_fields_match(): void
    {
        static::assertTrue(ParsedExcludeDefinitionMother::with()->equals(ParsedExcludeDefinitionMother::with()));
    }

    public function test_not_equal_when_access_method_differs(): void
    {
        static::assertFalse(ParsedExcludeDefinitionMother::with(
            accessMethod: 'gist',
        )->equals(ParsedExcludeDefinitionMother::with(accessMethod: 'btree')));
    }

    public function test_not_equal_when_deferrable_differs(): void
    {
        static::assertFalse(ParsedExcludeDefinitionMother::with(
            deferrable: true,
        )->equals(ParsedExcludeDefinitionMother::with(deferrable: false)));
    }

    public function test_not_equal_when_element_expression_differs(): void
    {
        static::assertFalse(ParsedExcludeDefinitionMother::with(elements: [[
            'expression' => 'a',
            'operator' => '=',
        ]])->equals(ParsedExcludeDefinitionMother::with(elements: [['expression' => 'b', 'operator' => '=']])));
    }

    public function test_not_equal_when_element_operator_differs(): void
    {
        static::assertFalse(ParsedExcludeDefinitionMother::with(elements: [[
            'expression' => 'a',
            'operator' => '=',
        ]])->equals(ParsedExcludeDefinitionMother::with(elements: [['expression' => 'a', 'operator' => '&&']])));
    }

    public function test_not_equal_when_element_order_differs(): void
    {
        static::assertFalse(ParsedExcludeDefinitionMother::with(elements: [
            ['expression' => 'a', 'operator' => '='],
            ['expression' => 'b', 'operator' => '&&'],
        ])->equals(ParsedExcludeDefinitionMother::with(elements: [
            ['expression' => 'b', 'operator' => '&&'],
            ['expression' => 'a', 'operator' => '='],
        ])));
    }

    public function test_not_equal_when_initially_deferred_differs(): void
    {
        static::assertFalse(ParsedExcludeDefinitionMother::with(
            deferrable: true,
            initiallyDeferred: true,
        )->equals(ParsedExcludeDefinitionMother::with(deferrable: true, initiallyDeferred: false)));
    }

    public function test_not_equal_when_predicate_differs(): void
    {
        static::assertFalse(ParsedExcludeDefinitionMother::with(
            predicate: 'a > 0',
        )->equals(ParsedExcludeDefinitionMother::with(predicate: 'a > 1')));
    }

    public function test_not_equal_when_predicate_presence_differs(): void
    {
        static::assertFalse(ParsedExcludeDefinitionMother::with(
            predicate: 'a > 0',
        )->equals(ParsedExcludeDefinitionMother::with(predicate: null)));
    }
}
