<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use Flow\PostgreSql\AST\Transformers\ExplainConfig;
use Flow\PostgreSql\Explain\Analyzer\PlanAnalyzer;
use Flow\PostgreSql\Explain\Analyzer\PlanSummary;
use Flow\PostgreSql\Explain\Plan\PlanNodeType;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\PostgreSql\DSL\binary_expr;
use function Flow\PostgreSql\DSL\cast;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\sql_explain_config;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;

final class PgSqlExplainTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{ExplainConfig, array{
     *     hasExecutionTime: bool,
     *     hasPlanningTime: bool,
     *     hasActualRows: bool,
     *     hasTiming: bool,
     *     hasBuffers: bool
     * }}>
     */
    public static function provideExplainConfigCombinations(): Generator
    {
        yield 'forAnalysis - full execution data' => [
            ExplainConfig::forAnalysis(),
            [
                'hasExecutionTime' => true,
                'hasPlanningTime' => true,
                'hasActualRows' => true,
                'hasTiming' => true,
                'hasBuffers' => true,
            ],
        ];

        yield 'forEstimate - no execution data' => [
            ExplainConfig::forEstimate(),
            [
                'hasExecutionTime' => false,
                'hasPlanningTime' => false,
                'hasActualRows' => false,
                'hasTiming' => false,
                'hasBuffers' => false,
            ],
        ];

        yield 'analyze with no buffers' => [
            ExplainConfig::forAnalysis()->withoutBuffers(),
            [
                'hasExecutionTime' => true,
                'hasPlanningTime' => true,
                'hasActualRows' => true,
                'hasTiming' => true,
                'hasBuffers' => false,
            ],
        ];

        yield 'analyze with no timing' => [
            ExplainConfig::forAnalysis()->withoutTiming(),
            [
                'hasExecutionTime' => true,
                'hasPlanningTime' => true,
                'hasActualRows' => true,
                'hasTiming' => false,
                'hasBuffers' => true,
            ],
        ];

        yield 'analyze with verbose' => [
            ExplainConfig::forAnalysis()->withVerbose(),
            [
                'hasExecutionTime' => true,
                'hasPlanningTime' => true,
                'hasActualRows' => true,
                'hasTiming' => true,
                'hasBuffers' => true,
            ],
        ];

        yield 'analyze without costs' => [
            ExplainConfig::forAnalysis()->withoutCosts(),
            [
                'hasExecutionTime' => true,
                'hasPlanningTime' => true,
                'hasActualRows' => true,
                'hasTiming' => true,
                'hasBuffers' => true,
            ],
        ];

        yield 'analyze without summary' => [
            ExplainConfig::forAnalysis()->withoutSummary(),
            [
                'hasExecutionTime' => false,
                'hasPlanningTime' => false,
                'hasActualRows' => true,
                'hasTiming' => true,
                'hasBuffers' => true,
            ],
        ];
    }

    public function test_explain_for_estimate(): void
    {
        $plan = $this
            ->pgsqlContext()
            ->client()
            ->explain(select(literal(1)), config: ExplainConfig::forEstimate());

        static::assertGreaterThanOrEqual(0.0, $plan->totalCost());
        static::assertNull($plan->executionTime());
        static::assertNull($plan->planningTime());
    }

    public function test_explain_returns_plan_for_select_query(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->temporaryTable('test_explain')
                    ->column(column('id', column_type_integer()))
                    ->column(column('name', column_type_text())),
            );

        $plan = $this
            ->pgsqlContext()
            ->client()
            ->explain(select(star())->from(table('test_explain')));

        static::assertNotNull($plan->executionTime());
        static::assertNotNull($plan->planningTime());
    }

    public function test_explain_returns_plan_with_parameters(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->temporaryTable('test_explain_params')
                    ->column(column('id', column_type_integer()))
                    ->column(column('name', column_type_text())),
            );

        $plan = $this
            ->pgsqlContext()
            ->client()
            ->explain(select(star())->from(table('test_explain_params'))->where(eq(col('id'), param(1))), ['42']);

        static::assertGreaterThanOrEqual(0.0, $plan->totalCost());
    }

    public function test_explain_returns_plan_with_raw_sql(): void
    {
        $plan = $this
            ->pgsqlContext()
            ->client()
            ->explain(
                select(binary_expr(cast(param(1), column_type_integer()), '+', cast(param(2), column_type_integer()))),
                ['10', '32'],
            );

        static::assertSame(PlanNodeType::RESULT, $plan->rootNode()->nodeType());
    }

    /**
     * @param array{
     *     hasExecutionTime: bool,
     *     hasPlanningTime: bool,
     *     hasActualRows: bool,
     *     hasTiming: bool,
     *     hasBuffers: bool
     * } $expected
     */
    #[DataProvider('provideExplainConfigCombinations')]
    public function test_explain_with_config_combinations(ExplainConfig $config, array $expected): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->temporaryTable('test_explain_combinations')
                    ->column(column('id', column_type_integer()))
                    ->column(column('name', column_type_text())),
            );
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                insert()->into('test_explain_combinations')->columns('id', 'name')->values(literal(1), literal('test')),
            );

        $plan = $this
            ->pgsqlContext()
            ->client()
            ->explain(select(star())->from(table('test_explain_combinations')), config: $config);

        if ($expected['hasExecutionTime']) {
            static::assertNotNull($plan->executionTime(), 'Expected executionTime to be present');
        } else {
            static::assertNull($plan->executionTime(), 'Expected executionTime to be null');
        }

        if ($expected['hasPlanningTime']) {
            static::assertNotNull($plan->planningTime(), 'Expected planningTime to be present');
        } else {
            static::assertNull($plan->planningTime(), 'Expected planningTime to be null');
        }

        if ($expected['hasActualRows']) {
            static::assertNotNull($plan->rootNode()->actualRows(), 'Expected actualRows to be present');
        } else {
            static::assertNull($plan->rootNode()->actualRows(), 'Expected actualRows to be null');
        }

        if ($expected['hasTiming']) {
            static::assertNotNull($plan->rootNode()->timing(), 'Expected timing to be present');
        } else {
            static::assertNull($plan->rootNode()->timing(), 'Expected timing to be null');
        }

        if ($expected['hasBuffers']) {
            static::assertNotNull($plan->rootNode()->buffers(), 'Expected buffers to be present');
        } else {
            static::assertNull($plan->rootNode()->buffers(), 'Expected buffers to be null');
        }

        static::assertGreaterThanOrEqual(0.0, $plan->totalCost());
    }

    public function test_explain_with_custom_config(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(create()->temporaryTable('test_explain_config')->column(column('id', column_type_integer())));

        $plan = $this
            ->pgsqlContext()
            ->client()
            ->explain(select(star())->from(table('test_explain_config')), config: sql_explain_config());

        static::assertNotNull($plan->executionTime());
        static::assertGreaterThanOrEqual(0.0, $plan->rootNode()->cost()->totalCost());
    }

    public function test_explain_without_analyze(): void
    {
        $plan = $this
            ->pgsqlContext()
            ->client()
            ->explain(select(literal(1)), config: sql_explain_config(analyze: false, buffers: false, timing: false));

        static::assertGreaterThanOrEqual(0.0, $plan->totalCost());
        static::assertNull($plan->executionTime());
        static::assertNull($plan->rootNode()->timing());
    }

    /**
     * @param array{
     *     hasExecutionTime: bool,
     *     hasPlanningTime: bool,
     *     hasActualRows: bool,
     *     hasTiming: bool,
     *     hasBuffers: bool
     * } $expected
     */
    #[DataProvider('provideExplainConfigCombinations')]
    public function test_plan_summary_with_config_combinations(ExplainConfig $config, array $expected): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->temporaryTable('test_summary_combinations')
                    ->column(column('id', column_type_integer()))
                    ->column(column('name', column_type_text())),
            );
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                insert()->into('test_summary_combinations')->columns('id', 'name')->values(literal(1), literal('test')),
            );

        $plan = $this
            ->pgsqlContext()
            ->client()
            ->explain(select(star())->from(table('test_summary_combinations')), config: $config);

        $analyzer = new PlanAnalyzer($plan);
        $summary = $analyzer->summary();

        if ($expected['hasExecutionTime']) {
            static::assertNotNull($summary->executionTime, 'Expected summary executionTime to be present');
        } else {
            static::assertNull($summary->executionTime, 'Expected summary executionTime to be null');
        }

        if ($expected['hasPlanningTime']) {
            static::assertNotNull($summary->planningTime, 'Expected summary planningTime to be present');
        } else {
            static::assertNull($summary->planningTime, 'Expected summary planningTime to be null');
        }

        if ($expected['hasActualRows']) {
            static::assertNotNull($summary->actualRows, 'Expected summary actualRows to be present');
        } else {
            static::assertNull($summary->actualRows, 'Expected summary actualRows to be null');
        }

        static::assertGreaterThanOrEqual(0, $summary->nodeCount);
        static::assertGreaterThanOrEqual(0, $summary->estimatedRows);
        static::assertGreaterThanOrEqual(0.0, $summary->totalCost);

        $normalized = $summary->normalize();
        $restored = PlanSummary::fromArray($normalized);
        static::assertEquals($summary, $restored);
    }
}
