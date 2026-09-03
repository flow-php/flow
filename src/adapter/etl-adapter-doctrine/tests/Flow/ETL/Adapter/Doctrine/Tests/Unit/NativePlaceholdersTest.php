<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Doctrine\DBAL\SQL\Parser\Visitor;
use Flow\ETL\Adapter\Doctrine\NativePlaceholders;
use Flow\ETL\Adapter\Doctrine\RewrittenSql;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use ReflectionClass;
use ReflectionMethod;

use function array_map;

final class NativePlaceholdersTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{string, string, int<0, max>, string, int<0, max>}>
     */
    public static function provide_placeholders(): Generator
    {
        yield 'named' => [
            'SELECT id FROM t WHERE id > :min',
            'SELECT id FROM t WHERE id > $1',
            1,
            'SELECT id FROM t WHERE id > ?',
            1,
        ];
        yield 'two named' => [
            'SELECT id FROM t WHERE a > :x AND b < :y',
            'SELECT id FROM t WHERE a > $1 AND b < $2',
            2,
            'SELECT id FROM t WHERE a > ? AND b < ?',
            2,
        ];
        yield 'positional' => [
            'SELECT id FROM t WHERE id > ?',
            'SELECT id FROM t WHERE id > $1',
            1,
            'SELECT id FROM t WHERE id > ?',
            1,
        ];
        yield 'cast operator is not a placeholder' => [
            'SELECT 1::text FROM t',
            'SELECT 1::text FROM t',
            0,
            'SELECT 1::text FROM t',
            0,
        ];
        yield 'quoted literal' => [
            "SELECT ':not a param' FROM t",
            "SELECT ':not a param' FROM t",
            0,
            "SELECT ':not a param' FROM t",
            0,
        ];
        yield 'line comment' => [
            'SELECT id FROM t -- :nope',
            'SELECT id FROM t -- :nope',
            0,
            'SELECT id FROM t -- :nope',
            0,
        ];
        yield 'block comment' => [
            'SELECT id FROM t /* :nope */',
            'SELECT id FROM t /* :nope */',
            0,
            'SELECT id FROM t /* :nope */',
            0,
        ];
        yield 'no placeholders' => ['SELECT id FROM t', 'SELECT id FROM t', 0, 'SELECT id FROM t', 0];
    }

    /**
     * @param int<0, max> $postgreSqlCount
     * @param int<0, max> $mysqliCount
     */
    #[DataProvider('provide_placeholders')]
    public function test_placeholders_are_rewritten_to_each_native_dialect(
        string $sql,
        string $postgreSql,
        int $postgreSqlCount,
        string $mysqli,
        int $mysqliCount,
    ): void {
        static::assertEquals(
            new RewrittenSql($postgreSql, $postgreSqlCount),
            (new NativePlaceholders())->toPostgreSql($sql),
        );
        static::assertEquals(new RewrittenSql($mysqli, $mysqliCount), (new NativePlaceholders())->toMysqli($sql));
    }

    public function test_dollar_quoting_is_rewritten_inside_because_dbal_does_not_tokenise_it(): void
    {
        // DBAL's parser does not know $$...$$, and DBAL's own pgsql driver runs the same parser on
        // every prepare(), so a :name inside dollar-quoting never worked through DBAL. The probe
        // must describe the statement DBAL executes, so it pins DBAL's behaviour, not PostgreSQL's
        // grammar. When a DBAL upgrade fixes the parser this test fails and the expectation flips.
        static::assertEquals(
            new RewrittenSql('SELECT $$ $1 $$ FROM t', 1),
            (new NativePlaceholders())->toPostgreSql('SELECT $$ :nope $$ FROM t'),
        );
    }

    public function test_the_dbal_sql_parser_contract_is_still_what_we_depend_on(): void
    {
        static::assertSame(
            ['acceptPositionalParameter', 'acceptNamedParameter', 'acceptOther'],
            array_map(
                static fn(ReflectionMethod $method): string => $method->getName(),
                (new ReflectionClass(Visitor::class))->getMethods(),
            ),
            'Doctrine\DBAL\SQL\Parser\Visitor changed shape - PlaceholderRewriter must follow',
        );
    }
}
