<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Parser;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Parser\{ExcludeDefinitionParser, ExpressionParser};
use PHPUnit\Framework\TestCase;

final class ExcludeDefinitionParserTest extends TestCase
{
    private ExcludeDefinitionParser $parser;

    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }

        $this->parser = new ExcludeDefinitionParser(new Parser(), new ExpressionParser(new Parser()));
    }

    public function test_lowercases_access_method() : void
    {
        self::assertSame('gist', $this->parser->parse('USING GiST (tsrange WITH &&)')->accessMethod);
    }

    public function test_parses_deferrable_initially_deferred() : void
    {
        $parsed = $this->parser->parse('USING btree (a WITH =) DEFERRABLE INITIALLY DEFERRED');

        self::assertTrue($parsed->deferrable);
        self::assertTrue($parsed->initiallyDeferred);
    }

    public function test_parses_deferrable_initially_immediate() : void
    {
        $parsed = $this->parser->parse('USING btree (a WITH =) DEFERRABLE INITIALLY IMMEDIATE');

        self::assertTrue($parsed->deferrable);
        self::assertFalse($parsed->initiallyDeferred);
    }

    public function test_parses_expression_element() : void
    {
        $parsed = $this->parser->parse('USING gist (tsrange(start_ts, end_ts) WITH &&)');

        self::assertCount(1, $parsed->elements);
        self::assertSame('&&', $parsed->elements[0]['operator']);
        self::assertStringContainsString('tsrange', $parsed->elements[0]['expression']);
    }

    public function test_parses_multiple_elements_with_different_operators() : void
    {
        $parsed = $this->parser->parse('USING gist (room_id WITH =, during WITH &&)');

        self::assertSame('gist', $parsed->accessMethod);
        self::assertSame([
            ['expression' => 'room_id', 'operator' => '='],
            ['expression' => 'during', 'operator' => '&&'],
        ], $parsed->elements);
    }

    public function test_parses_predicate_and_normalizes_implicit_casts() : void
    {
        $normalized = $this->parser->parse("USING btree (room_id WITH =) WHERE (status = 'active')");
        $canonicalized = $this->parser->parse("USING btree (room_id WITH =) WHERE (((status)::text = 'active'::text))");

        self::assertSame("status = 'active'", $normalized->predicate);
        self::assertTrue($normalized->equals($canonicalized));
    }

    public function test_preserves_operator_case_for_case_sensitive_operators() : void
    {
        self::assertSame('&&', $this->parser->parse('USING gist (a WITH &&)')->elements[0]['operator']);
    }

    public function test_throws_on_invalid_input() : void
    {
        $this->expectException(\Throwable::class);

        $this->parser->parse('not a constraint definition at all');
    }

    public function test_tolerates_missing_exclude_prefix() : void
    {
        self::assertTrue(
            $this->parser->parse('USING btree (room_id WITH =)')
                ->equals($this->parser->parse('EXCLUDE USING btree (room_id WITH =)'))
        );
    }
}
