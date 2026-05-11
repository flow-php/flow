<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Parser;

use Flow\PostgreSql\Parser\ExpressionParser;
use Flow\PostgreSql\Parser\TriggerDefinitionParser;
use PHPUnit\Framework\TestCase;

final class TriggerDefinitionParserTest extends TestCase
{
    private TriggerDefinitionParser $parser;

    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }

        $this->parser = new TriggerDefinitionParser(new ExpressionParser());
    }

    public function test_handles_nested_boolean_expression(): void
    {
        static::assertSame(
            'new.value > 0 AND new.value < 100',
            $this->parser->parseWhenClause(
                'CREATE TRIGGER t BEFORE UPDATE ON s.tbl FOR EACH ROW WHEN (((new.value > 0) AND (new.value < 100))) EXECUTE FUNCTION s.fn()',
            ),
        );
    }

    public function test_handles_string_literal_with_embedded_parens_and_escapes(): void
    {
        static::assertSame(
            "new.label = 'O''Brien)'",
            $this->parser->parseWhenClause(
                "CREATE TRIGGER t BEFORE UPDATE ON s.tbl FOR EACH ROW WHEN (new.label = 'O''Brien)') EXECUTE FUNCTION s.fn()",
            ),
        );
    }

    public function test_normalizes_implicit_casts(): void
    {
        static::assertSame(
            "new.status = 'active'",
            $this->parser->parseWhenClause(
                "CREATE TRIGGER t BEFORE UPDATE ON s.tbl FOR EACH ROW WHEN (((new.status)::text = 'active'::text)) EXECUTE FUNCTION s.fn()",
            ),
        );
    }

    public function test_returns_expression_referencing_both_new_and_old(): void
    {
        static::assertSame(
            'new.value <> old.value',
            $this->parser->parseWhenClause(
                'CREATE TRIGGER t BEFORE UPDATE ON s.tbl FOR EACH ROW WHEN (new.value <> old.value) EXECUTE FUNCTION s.fn()',
            ),
        );
    }

    public function test_returns_null_when_no_when_clause_present(): void
    {
        static::assertNull($this->parser->parseWhenClause(
            'CREATE TRIGGER t BEFORE UPDATE ON s.tbl FOR EACH ROW EXECUTE FUNCTION s.fn()',
        ));
    }

    public function test_throws_on_non_trigger_statement(): void
    {
        $this->expectException(\Throwable::class);

        $this->parser->parseWhenClause('SELECT 1');
    }
}
