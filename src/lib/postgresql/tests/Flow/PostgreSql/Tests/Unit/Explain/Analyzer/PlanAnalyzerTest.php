<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Explain\Analyzer;

use function Flow\PostgreSql\DSL\{sql_analyze, sql_explain_parse};
use Flow\PostgreSql\Explain\Analyzer\{InsightSeverity, InsightType};
use PHPUnit\Framework\TestCase;

final class PlanAnalyzerTest extends TestCase
{
    public function test_all_insights_aggregates_all_insight_types() : void
    {
        $json = <<<'JSON'
[
  {
    "Plan": {
      "Node Type": "Seq Scan",
      "Relation Name": "orders",
      "Alias": "o",
      "Startup Cost": 0.00,
      "Total Cost": 1000.00,
      "Plan Rows": 100,
      "Plan Width": 50,
      "Actual Startup Time": 0.1,
      "Actual Total Time": 100.0,
      "Actual Rows": 10000,
      "Actual Loops": 1,
      "Rows Removed by Filter": 50000,
      "Filter": "(status = 'pending')"
    },
    "Planning Time": 0.5,
    "Execution Time": 100.0
  }
]
JSON;

        $plan = sql_explain_parse($json);
        $analyzer = sql_analyze($plan);

        $insights = $analyzer->allInsights();

        self::assertNotEmpty($insights);
        $types = \array_map(static fn ($i) => $i->type, $insights);
        self::assertContains(InsightType::SLOW_NODE, $types);
        self::assertContains(InsightType::SEQUENTIAL_SCAN, $types);
        self::assertContains(InsightType::ESTIMATE_MISMATCH, $types);
        self::assertContains(InsightType::INEFFICIENT_FILTER, $types);
    }

    public function test_disk_reads_detects_shared_reads() : void
    {
        $json = <<<'JSON'
[
  {
    "Plan": {
      "Node Type": "Seq Scan",
      "Relation Name": "large_table",
      "Startup Cost": 0.00,
      "Total Cost": 500.00,
      "Plan Rows": 10000,
      "Plan Width": 100,
      "Shared Hit Blocks": 50,
      "Shared Read Blocks": 450
    }
  }
]
JSON;

        $plan = sql_explain_parse($json);
        $analyzer = sql_analyze($plan);

        $insights = $analyzer->diskReads();

        self::assertCount(1, $insights);
        self::assertSame(InsightType::DISK_READ, $insights[0]->type);
        self::assertSame(InsightSeverity::CRITICAL, $insights[0]->severity);
        self::assertSame(450, $insights[0]->metrics['blocks_read']);
    }

    public function test_estimate_mismatch_detects_significant_differences() : void
    {
        $json = <<<'JSON'
[
  {
    "Plan": {
      "Node Type": "Index Scan",
      "Relation Name": "orders",
      "Index Name": "orders_status_idx",
      "Startup Cost": 0.00,
      "Total Cost": 10.00,
      "Plan Rows": 10,
      "Plan Width": 50,
      "Actual Rows": 5000,
      "Actual Loops": 1
    }
  }
]
JSON;

        $plan = sql_explain_parse($json);
        $analyzer = sql_analyze($plan);

        $insights = $analyzer->estimateMismatches();

        self::assertCount(1, $insights);
        self::assertSame(InsightType::ESTIMATE_MISMATCH, $insights[0]->type);
        self::assertSame(InsightSeverity::CRITICAL, $insights[0]->severity);
        self::assertSame(500.0, $insights[0]->metrics['mismatch_ratio']);
    }

    public function test_estimate_mismatch_respects_threshold() : void
    {
        $json = <<<'JSON'
[
  {
    "Plan": {
      "Node Type": "Index Scan",
      "Relation Name": "users",
      "Index Name": "users_id_idx",
      "Startup Cost": 0.00,
      "Total Cost": 10.00,
      "Plan Rows": 100,
      "Plan Width": 50,
      "Actual Rows": 150,
      "Actual Loops": 1
    }
  }
]
JSON;

        $plan = sql_explain_parse($json);
        $analyzer = sql_analyze($plan);

        $insights = $analyzer->estimateMismatches(threshold: 2.0);

        self::assertEmpty($insights);
    }

    public function test_external_sorts_detects_disk_based_sorting() : void
    {
        $json = <<<'JSON'
[
  {
    "Plan": {
      "Node Type": "Sort",
      "Sort Key": ["orders.created_at"],
      "Sort Method": "external merge",
      "Sort Space Used": 10240,
      "Sort Space Type": "Disk",
      "Startup Cost": 1000.00,
      "Total Cost": 1500.00,
      "Plan Rows": 100000,
      "Plan Width": 100
    }
  }
]
JSON;

        $plan = sql_explain_parse($json);
        $analyzer = sql_analyze($plan);

        $insights = $analyzer->externalSorts();

        self::assertCount(1, $insights);
        self::assertSame(InsightType::EXTERNAL_SORT, $insights[0]->type);
        self::assertSame(InsightSeverity::WARNING, $insights[0]->severity);
        self::assertSame(10240, $insights[0]->metrics['space_used_kb']);
    }

    public function test_inefficient_filters_detects_high_removal_ratio() : void
    {
        $json = <<<'JSON'
[
  {
    "Plan": {
      "Node Type": "Seq Scan",
      "Relation Name": "events",
      "Filter": "(type = 'rare_event')",
      "Startup Cost": 0.00,
      "Total Cost": 100.00,
      "Plan Rows": 1000,
      "Plan Width": 50,
      "Actual Rows": 10,
      "Actual Loops": 1,
      "Rows Removed by Filter": 9990
    }
  }
]
JSON;

        $plan = sql_explain_parse($json);
        $analyzer = sql_analyze($plan);

        $insights = $analyzer->inefficientFilters();

        self::assertCount(1, $insights);
        self::assertSame(InsightType::INEFFICIENT_FILTER, $insights[0]->type);
        self::assertSame(InsightSeverity::CRITICAL, $insights[0]->severity);
        self::assertEqualsWithDelta(0.999, $insights[0]->metrics['removal_ratio'], 0.001);
    }

    public function test_low_cache_hits_detects_poor_cache_performance() : void
    {
        $json = <<<'JSON'
[
  {
    "Plan": {
      "Node Type": "Index Scan",
      "Relation Name": "products",
      "Index Name": "products_pkey",
      "Startup Cost": 0.00,
      "Total Cost": 50.00,
      "Plan Rows": 1000,
      "Plan Width": 100,
      "Shared Hit Blocks": 10,
      "Shared Read Blocks": 90
    }
  }
]
JSON;

        $plan = sql_explain_parse($json);
        $analyzer = sql_analyze($plan);

        $insights = $analyzer->lowCacheHits();

        self::assertCount(1, $insights);
        self::assertSame(InsightType::LOW_CACHE_HIT, $insights[0]->type);
        self::assertSame(InsightSeverity::CRITICAL, $insights[0]->severity);
        self::assertEqualsWithDelta(0.1, $insights[0]->metrics['hit_ratio'], 0.01);
    }

    public function test_no_insights_for_well_optimized_plan() : void
    {
        $json = <<<'JSON'
[
  {
    "Plan": {
      "Node Type": "Index Scan",
      "Relation Name": "users",
      "Index Name": "users_pkey",
      "Startup Cost": 0.00,
      "Total Cost": 8.00,
      "Plan Rows": 1,
      "Plan Width": 50,
      "Actual Rows": 1,
      "Actual Loops": 1,
      "Shared Hit Blocks": 3,
      "Shared Read Blocks": 0
    },
    "Planning Time": 0.1,
    "Execution Time": 0.05
  }
]
JSON;

        $plan = sql_explain_parse($json);
        $analyzer = sql_analyze($plan);

        self::assertEmpty($analyzer->sequentialScans());
        self::assertEmpty($analyzer->externalSorts());
        self::assertEmpty($analyzer->estimateMismatches());
        self::assertEmpty($analyzer->inefficientFilters());
        self::assertEmpty($analyzer->diskReads());
        self::assertEmpty($analyzer->lowCacheHits());
    }

    public function test_sequential_scans_finds_all_seq_scans() : void
    {
        $json = <<<'JSON'
[
  {
    "Plan": {
      "Node Type": "Nested Loop",
      "Startup Cost": 0.00,
      "Total Cost": 200.00,
      "Plan Rows": 1000,
      "Plan Width": 100,
      "Plans": [
        {
          "Node Type": "Seq Scan",
          "Parent Relationship": "Outer",
          "Relation Name": "users",
          "Startup Cost": 0.00,
          "Total Cost": 50.00,
          "Plan Rows": 100,
          "Plan Width": 50
        },
        {
          "Node Type": "Seq Scan",
          "Parent Relationship": "Inner",
          "Relation Name": "orders",
          "Startup Cost": 0.00,
          "Total Cost": 75.00,
          "Plan Rows": 500,
          "Plan Width": 80
        }
      ]
    }
  }
]
JSON;

        $plan = sql_explain_parse($json);
        $analyzer = sql_analyze($plan);

        $insights = $analyzer->sequentialScans();

        self::assertCount(2, $insights);
        self::assertSame(InsightType::SEQUENTIAL_SCAN, $insights[0]->type);
        self::assertSame(InsightType::SEQUENTIAL_SCAN, $insights[1]->type);
    }

    public function test_slowest_nodes_returns_top_n_by_time() : void
    {
        $json = <<<'JSON'
[
  {
    "Plan": {
      "Node Type": "Hash Join",
      "Startup Cost": 100.00,
      "Total Cost": 500.00,
      "Plan Rows": 1000,
      "Plan Width": 100,
      "Actual Startup Time": 10.0,
      "Actual Total Time": 50.0,
      "Actual Rows": 1000,
      "Actual Loops": 1,
      "Plans": [
        {
          "Node Type": "Seq Scan",
          "Parent Relationship": "Outer",
          "Relation Name": "users",
          "Startup Cost": 0.00,
          "Total Cost": 50.00,
          "Plan Rows": 100,
          "Plan Width": 50,
          "Actual Startup Time": 0.1,
          "Actual Total Time": 5.0,
          "Actual Rows": 100,
          "Actual Loops": 1
        },
        {
          "Node Type": "Hash",
          "Parent Relationship": "Inner",
          "Startup Cost": 25.00,
          "Total Cost": 25.00,
          "Plan Rows": 500,
          "Plan Width": 50,
          "Actual Startup Time": 5.0,
          "Actual Total Time": 20.0,
          "Actual Rows": 500,
          "Actual Loops": 1
        }
      ]
    },
    "Execution Time": 50.0
  }
]
JSON;

        $plan = sql_explain_parse($json);
        $analyzer = sql_analyze($plan);

        $insights = $analyzer->slowestNodes(limit: 2);

        self::assertCount(2, $insights);
        self::assertSame(InsightType::SLOW_NODE, $insights[0]->type);
        self::assertGreaterThanOrEqual($insights[1]->metrics['time_ms'], $insights[0]->metrics['time_ms']);
    }

    public function test_summary_provides_aggregate_statistics() : void
    {
        $json = <<<'JSON'
[
  {
    "Plan": {
      "Node Type": "Nested Loop",
      "Startup Cost": 0.00,
      "Total Cost": 200.00,
      "Plan Rows": 1000,
      "Plan Width": 100,
      "Plans": [
        {
          "Node Type": "Seq Scan",
          "Parent Relationship": "Outer",
          "Relation Name": "users",
          "Startup Cost": 0.00,
          "Total Cost": 50.00,
          "Plan Rows": 100,
          "Plan Width": 50,
          "Shared Hit Blocks": 20,
          "Shared Read Blocks": 5
        },
        {
          "Node Type": "Index Scan",
          "Parent Relationship": "Inner",
          "Relation Name": "orders",
          "Index Name": "orders_user_id_idx",
          "Startup Cost": 0.00,
          "Total Cost": 10.00,
          "Plan Rows": 10,
          "Plan Width": 80,
          "Shared Hit Blocks": 100,
          "Shared Read Blocks": 0
        }
      ]
    },
    "Planning Time": 0.5,
    "Execution Time": 25.0
  }
]
JSON;

        $plan = sql_explain_parse($json);
        $analyzer = sql_analyze($plan);

        $summary = $analyzer->summary();

        self::assertSame(200.0, $summary->totalCost);
        self::assertSame(25.0, $summary->executionTime);
        self::assertSame(0.5, $summary->planningTime);
        self::assertSame(3, $summary->nodeCount);
        self::assertSame(1, $summary->sequentialScanCount);
        self::assertSame(1, $summary->indexScanCount);
        self::assertFalse($summary->hasExternalSort);
        self::assertTrue($summary->hasDiskReads);
        self::assertEqualsWithDelta(0.96, $summary->overallCacheHitRatio, 0.01);
    }

    public function test_summary_with_external_sort() : void
    {
        $json = <<<'JSON'
[
  {
    "Plan": {
      "Node Type": "Sort",
      "Sort Key": ["created_at"],
      "Sort Method": "external merge",
      "Sort Space Used": 5000,
      "Sort Space Type": "Disk",
      "Startup Cost": 500.00,
      "Total Cost": 600.00,
      "Plan Rows": 50000,
      "Plan Width": 100
    }
  }
]
JSON;

        $plan = sql_explain_parse($json);
        $analyzer = sql_analyze($plan);

        $summary = $analyzer->summary();

        self::assertTrue($summary->hasExternalSort);
    }
}
