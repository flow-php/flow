<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Parser;

use Flow\PostgreSql\Parser\ExcludeDefinitionParser;
use Flow\PostgreSql\Parser\ExpressionParser;
use PHPUnit\Framework\TestCase;
use Throwable;

use function extension_loaded;

final class ExcludeDefinitionParserTest extends TestCase
{
    private ExcludeDefinitionParser $parser;

    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }

        $this->parser = new ExcludeDefinitionParser(new ExpressionParser());
    }

    public function test_lowercases_access_method(): void
    {
        static::assertSame('gist', $this->parser->parse('USING GiST (tsrange WITH &&)')->accessMethod);
    }

    public function test_parses_deferrable_initially_deferred(): void
    {
        $parsed = $this->parser->parse('USING btree (a WITH =) DEFERRABLE INITIALLY DEFERRED');

        static::assertTrue($parsed->deferrable);
        static::assertTrue($parsed->initiallyDeferred);
    }

    public function test_parses_deferrable_initially_immediate(): void
    {
        $parsed = $this->parser->parse('USING btree (a WITH =) DEFERRABLE INITIALLY IMMEDIATE');

        static::assertTrue($parsed->deferrable);
        static::assertFalse($parsed->initiallyDeferred);
    }

    public function test_parses_expression_element(): void
    {
        $parsed = $this->parser->parse('USING gist (tsrange(start_ts, end_ts) WITH &&)');

        static::assertCount(1, $parsed->elements);
        static::assertSame('&&', $parsed->elements[0]['operator']);
        static::assertStringContainsString('tsrange', $parsed->elements[0]['expression']);
    }

    public function test_parses_multiple_elements_with_different_operators(): void
    {
        $parsed = $this->parser->parse('USING gist (room_id WITH =, during WITH &&)');

        static::assertSame('gist', $parsed->accessMethod);
        static::assertSame(
            [
                ['expression' => 'room_id', 'operator' => '='],
                ['expression' => 'during', 'operator' => '&&'],
            ],
            $parsed->elements,
        );
    }

    public function test_parses_predicate_and_normalizes_implicit_casts(): void
    {
        $normalized = $this->parser->parse("USING btree (room_id WITH =) WHERE (status = 'active')");
        $canonicalized = $this->parser->parse("USING btree (room_id WITH =) WHERE (((status)::text = 'active'::text))");

        static::assertSame("status = 'active'", $normalized->predicate);
        static::assertTrue($normalized->equals($canonicalized));
    }

    public function test_preserves_operator_case_for_case_sensitive_operators(): void
    {
        static::assertSame('&&', $this->parser->parse('USING gist (a WITH &&)')->elements[0]['operator']);
    }

    public function test_throws_on_invalid_input(): void
    {
        $this->expectException(Throwable::class);

        $this->parser->parse('not a constraint definition at all');
    }

    public function test_tolerates_missing_exclude_prefix(): void
    {
        static::assertTrue(
            $this->parser
                ->parse('USING btree (room_id WITH =)')
                ->equals($this->parser->parse('EXCLUDE USING btree (room_id WITH =)')),
        );
    }
}
