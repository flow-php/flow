<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Explain;

use Flow\PostgreSql\Exception\ExplainParseException;
use Flow\PostgreSql\Explain\ExplainParser;
use Flow\PostgreSql\Explain\Plan\PlanNodeType;
use PHPUnit\Framework\TestCase;

final class ExplainParserTest extends TestCase
{
    public function test_parse_basic_seq_scan(): void
    {
        $json = <<<'JSON'
            [
              {
                "Plan": {
                  "Node Type": "Seq Scan",
                  "Relation Name": "users",
                  "Alias": "users",
                  "Startup Cost": 0.00,
                  "Total Cost": 35.50,
                  "Plan Rows": 2550,
                  "Plan Width": 4
                },
                "Planning Time": 0.123,
                "Execution Time": 1.456
              }
            ]
            JSON;

        $parser = new ExplainParser();
        $plan = $parser->parse($json);

        static::assertSame(PlanNodeType::SEQ_SCAN, $plan->rootNode()->nodeType());
        static::assertSame('users', $plan->rootNode()->relationName());
        static::assertSame(0.0, $plan->rootNode()->cost()->startupCost());
        static::assertSame(35.50, $plan->rootNode()->cost()->totalCost());
        static::assertSame(2550, $plan->rootNode()->estimatedRows());
        static::assertSame(4, $plan->rootNode()->rowWidth());
        static::assertSame(0.123, $plan->planningTime());
        static::assertSame(1.456, $plan->executionTime());
        static::assertSame(35.50, $plan->totalCost());
    }

    public function test_parse_empty_array_throws_exception(): void
    {
        $this->expectException(ExplainParseException::class);

        $parser = new ExplainParser();
        $parser->parse('[]');
    }

    public function test_parse_external_sort(): void
    {
        $json = <<<'JSON'
            [
              {
                "Plan": {
                  "Node Type": "Sort",
                  "Sort Key": ["large_table.column"],
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

        $parser = new ExplainParser();
        $plan = $parser->parse($json);

        static::assertTrue($plan->rootNode()->usesExternalSort());
    }

    public function test_parse_index_scan(): void
    {
        $json = <<<'JSON'
            [
              {
                "Plan": {
                  "Node Type": "Index Scan",
                  "Index Name": "users_email_idx",
                  "Index Cond": "(email = 'test@example.com')",
                  "Relation Name": "users",
                  "Schema": "public",
                  "Scan Direction": "Forward",
                  "Startup Cost": 0.00,
                  "Total Cost": 8.17,
                  "Plan Rows": 1,
                  "Plan Width": 4
                }
              }
            ]
            JSON;

        $parser = new ExplainParser();
        $plan = $parser->parse($json);

        static::assertSame(PlanNodeType::INDEX_SCAN, $plan->rootNode()->nodeType());
        static::assertSame('users_email_idx', $plan->rootNode()->indexName());
        static::assertSame("(email = 'test@example.com')", $plan->rootNode()->indexCond());
        static::assertSame('public', $plan->rootNode()->schema());
        static::assertSame('Forward', $plan->rootNode()->scanDirection());
        static::assertTrue($plan->rootNode()->isIndexScan());
        static::assertFalse($plan->rootNode()->isSequentialScan());
    }

    public function test_parse_invalid_json_throws_exception(): void
    {
        $this->expectException(ExplainParseException::class);

        $parser = new ExplainParser();
        $parser->parse('invalid json');
    }

    public function test_parse_join_type(): void
    {
        $json = <<<'JSON'
            [
              {
                "Plan": {
                  "Node Type": "Nested Loop",
                  "Join Type": "Left",
                  "Startup Cost": 0.00,
                  "Total Cost": 100.00,
                  "Plan Rows": 100,
                  "Plan Width": 8,
                  "Plans": [
                    {
                      "Node Type": "Seq Scan",
                      "Relation Name": "users",
                      "Startup Cost": 0.00,
                      "Total Cost": 8.00,
                      "Plan Rows": 100,
                      "Plan Width": 4
                    },
                    {
                      "Node Type": "Index Scan",
                      "Index Name": "orders_user_id_idx",
                      "Relation Name": "orders",
                      "Startup Cost": 0.00,
                      "Total Cost": 0.92,
                      "Plan Rows": 1,
                      "Plan Width": 4
                    }
                  ]
                }
              }
            ]
            JSON;

        $parser = new ExplainParser();
        $plan = $parser->parse($json);

        static::assertSame(PlanNodeType::NESTED_LOOP, $plan->rootNode()->nodeType());
        static::assertSame('Left', $plan->rootNode()->joinType());
    }

    public function test_parse_missing_plan_throws_exception(): void
    {
        $this->expectException(ExplainParseException::class);

        $parser = new ExplainParser();
        $parser->parse('[{"Planning Time": 0.123}]');
    }

    public function test_parse_nested_plan(): void
    {
        $json = <<<'JSON'
            [
              {
                "Plan": {
                  "Node Type": "Hash Join",
                  "Hash Cond": "(o.user_id = u.id)",
                  "Startup Cost": 10.00,
                  "Total Cost": 100.00,
                  "Plan Rows": 500,
                  "Plan Width": 8,
                  "Plans": [
                    {
                      "Node Type": "Seq Scan",
                      "Parent Relationship": "Outer",
                      "Relation Name": "orders",
                      "Startup Cost": 0.00,
                      "Total Cost": 35.50,
                      "Plan Rows": 500,
                      "Plan Width": 4
                    },
                    {
                      "Node Type": "Hash",
                      "Parent Relationship": "Inner",
                      "Startup Cost": 8.00,
                      "Total Cost": 8.00,
                      "Plan Rows": 100,
                      "Plan Width": 4,
                      "Plans": [
                        {
                          "Node Type": "Seq Scan",
                          "Parent Relationship": "Outer",
                          "Relation Name": "users",
                          "Startup Cost": 0.00,
                          "Total Cost": 8.00,
                          "Plan Rows": 100,
                          "Plan Width": 4
                        }
                      ]
                    }
                  ]
                }
              }
            ]
            JSON;

        $parser = new ExplainParser();
        $plan = $parser->parse($json);

        static::assertSame(PlanNodeType::HASH_JOIN, $plan->rootNode()->nodeType());
        static::assertSame('(o.user_id = u.id)', $plan->rootNode()->hashCond());
        static::assertCount(2, $plan->rootNode()->children());

        $outerChild = $plan->rootNode()->children()[0];
        static::assertSame(PlanNodeType::SEQ_SCAN, $outerChild->nodeType());
        static::assertSame('orders', $outerChild->relationName());

        $innerChild = $plan->rootNode()->children()[1];
        static::assertSame(PlanNodeType::HASH, $innerChild->nodeType());
        static::assertCount(1, $innerChild->children());
    }

    public function test_parse_sort_node(): void
    {
        $json = <<<'JSON'
            [
              {
                "Plan": {
                  "Node Type": "Sort",
                  "Sort Key": ["users.name", "users.id DESC"],
                  "Sort Method": "quicksort",
                  "Sort Space Used": 25,
                  "Sort Space Type": "Memory",
                  "Startup Cost": 10.00,
                  "Total Cost": 10.25,
                  "Plan Rows": 100,
                  "Plan Width": 4,
                  "Plans": [
                    {
                      "Node Type": "Seq Scan",
                      "Relation Name": "users",
                      "Startup Cost": 0.00,
                      "Total Cost": 8.00,
                      "Plan Rows": 100,
                      "Plan Width": 4
                    }
                  ]
                }
              }
            ]
            JSON;

        $parser = new ExplainParser();
        $plan = $parser->parse($json);

        static::assertSame(PlanNodeType::SORT, $plan->rootNode()->nodeType());
        static::assertSame('users.name, users.id DESC', $plan->rootNode()->sortKey());
        static::assertSame('quicksort', $plan->rootNode()->sortMethod());
        static::assertSame(25, $plan->rootNode()->sortSpaceUsed());
        static::assertSame('Memory', $plan->rootNode()->sortSpaceType());
        static::assertFalse($plan->rootNode()->usesExternalSort());
    }

    public function test_parse_with_analyze_data(): void
    {
        $json = <<<'JSON'
            [
              {
                "Plan": {
                  "Node Type": "Seq Scan",
                  "Relation Name": "users",
                  "Startup Cost": 0.00,
                  "Total Cost": 35.50,
                  "Plan Rows": 100,
                  "Plan Width": 4,
                  "Actual Startup Time": 0.012,
                  "Actual Total Time": 1.234,
                  "Actual Rows": 150,
                  "Actual Loops": 1
                },
                "Planning Time": 0.123,
                "Execution Time": 1.456
              }
            ]
            JSON;

        $parser = new ExplainParser();
        $plan = $parser->parse($json);

        static::assertSame(150, $plan->rootNode()->actualRows());
        static::assertSame(1, $plan->rootNode()->actualLoops());
        $timing = $plan->rootNode()->timing();
        static::assertNotNull($timing);
        static::assertSame(0.012, $timing->startupTime());
        static::assertSame(1.234, $timing->totalTime());
    }

    public function test_parse_with_buffers_data(): void
    {
        $json = <<<'JSON'
            [
              {
                "Plan": {
                  "Node Type": "Index Scan",
                  "Index Name": "users_pkey",
                  "Relation Name": "users",
                  "Startup Cost": 0.00,
                  "Total Cost": 8.17,
                  "Plan Rows": 1,
                  "Plan Width": 4,
                  "Shared Hit Blocks": 100,
                  "Shared Read Blocks": 10
                }
              }
            ]
            JSON;

        $parser = new ExplainParser();
        $plan = $parser->parse($json);

        $buffers = $plan->rootNode()->buffers();
        static::assertNotNull($buffers);
        static::assertSame(100, $buffers->sharedHit());
        static::assertSame(10, $buffers->sharedRead());
        static::assertSame(110, $buffers->totalSharedBlocks());
    }

    public function test_parse_with_filter(): void
    {
        $json = <<<'JSON'
            [
              {
                "Plan": {
                  "Node Type": "Seq Scan",
                  "Relation Name": "users",
                  "Filter": "(active = true)",
                  "Rows Removed by Filter": 500,
                  "Startup Cost": 0.00,
                  "Total Cost": 35.50,
                  "Plan Rows": 100,
                  "Plan Width": 4,
                  "Actual Rows": 100,
                  "Actual Loops": 1
                }
              }
            ]
            JSON;

        $parser = new ExplainParser();
        $plan = $parser->parse($json);

        static::assertSame('(active = true)', $plan->rootNode()->filter());
        static::assertSame(500, $plan->rootNode()->rowsRemovedByFilter());
    }

    public function test_parse_with_memory_info(): void
    {
        $json = <<<'JSON'
            [
              {
                "Plan": {
                  "Node Type": "Seq Scan",
                  "Relation Name": "users",
                  "Startup Cost": 0.00,
                  "Total Cost": 35.50,
                  "Plan Rows": 100,
                  "Plan Width": 4
                },
                "Planning Time": 0.5,
                "Execution Time": 2.5,
                "Memory Used": 1024,
                "Memory Peak": 2048
              }
            ]
            JSON;

        $parser = new ExplainParser();
        $plan = $parser->parse($json);

        static::assertSame(1024, $plan->memoryUsed());
        static::assertSame(2048, $plan->memoryPeak());
    }

    public function test_parse_with_temp_buffers(): void
    {
        $json = <<<'JSON'
            [
              {
                "Plan": {
                  "Node Type": "Hash",
                  "Startup Cost": 100.00,
                  "Total Cost": 100.00,
                  "Plan Rows": 1000,
                  "Plan Width": 100,
                  "Shared Hit Blocks": 50,
                  "Shared Read Blocks": 10,
                  "Temp Read Blocks": 200,
                  "Temp Written Blocks": 200
                }
              }
            ]
            JSON;

        $parser = new ExplainParser();
        $plan = $parser->parse($json);

        $buffers = $plan->rootNode()->buffers();
        static::assertNotNull($buffers);
        static::assertTrue($buffers->hasDiskSpill());
        static::assertSame(200, $buffers->tempRead());
        static::assertSame(200, $buffers->tempWritten());
        static::assertSame(400, $buffers->tempBlocks());
    }

    public function test_plan_all_nodes(): void
    {
        $json = <<<'JSON'
            [
              {
                "Plan": {
                  "Node Type": "Hash Join",
                  "Startup Cost": 10.00,
                  "Total Cost": 100.00,
                  "Plan Rows": 500,
                  "Plan Width": 8,
                  "Plans": [
                    {
                      "Node Type": "Seq Scan",
                      "Relation Name": "orders",
                      "Startup Cost": 0.00,
                      "Total Cost": 35.50,
                      "Plan Rows": 500,
                      "Plan Width": 4
                    },
                    {
                      "Node Type": "Hash",
                      "Startup Cost": 8.00,
                      "Total Cost": 8.00,
                      "Plan Rows": 100,
                      "Plan Width": 4,
                      "Plans": [
                        {
                          "Node Type": "Seq Scan",
                          "Relation Name": "users",
                          "Startup Cost": 0.00,
                          "Total Cost": 8.00,
                          "Plan Rows": 100,
                          "Plan Width": 4
                        }
                      ]
                    }
                  ]
                }
              }
            ]
            JSON;

        $parser = new ExplainParser();
        $plan = $parser->parse($json);

        $allNodes = $plan->allNodes();
        static::assertCount(4, $allNodes);
    }

    public function test_plan_nodes_by_type(): void
    {
        $json = <<<'JSON'
            [
              {
                "Plan": {
                  "Node Type": "Hash Join",
                  "Startup Cost": 10.00,
                  "Total Cost": 100.00,
                  "Plan Rows": 500,
                  "Plan Width": 8,
                  "Plans": [
                    {
                      "Node Type": "Seq Scan",
                      "Relation Name": "orders",
                      "Startup Cost": 0.00,
                      "Total Cost": 35.50,
                      "Plan Rows": 500,
                      "Plan Width": 4
                    },
                    {
                      "Node Type": "Hash",
                      "Startup Cost": 8.00,
                      "Total Cost": 8.00,
                      "Plan Rows": 100,
                      "Plan Width": 4,
                      "Plans": [
                        {
                          "Node Type": "Seq Scan",
                          "Relation Name": "users",
                          "Startup Cost": 0.00,
                          "Total Cost": 8.00,
                          "Plan Rows": 100,
                          "Plan Width": 4
                        }
                      ]
                    }
                  ]
                }
              }
            ]
            JSON;

        $parser = new ExplainParser();
        $plan = $parser->parse($json);

        $seqScans = $plan->nodesByType(PlanNodeType::SEQ_SCAN);
        static::assertCount(2, $seqScans);

        $hashNodes = $plan->nodesByType(PlanNodeType::HASH);
        static::assertCount(1, $hashNodes);
    }

    public function test_plan_total_time(): void
    {
        $json = <<<'JSON'
            [
              {
                "Plan": {
                  "Node Type": "Seq Scan",
                  "Relation Name": "users",
                  "Startup Cost": 0.00,
                  "Total Cost": 35.50,
                  "Plan Rows": 100,
                  "Plan Width": 4
                },
                "Planning Time": 0.5,
                "Execution Time": 2.5
              }
            ]
            JSON;

        $parser = new ExplainParser();
        $plan = $parser->parse($json);

        static::assertSame(3.0, $plan->totalTime());
    }

    public function test_row_estimate_accuracy(): void
    {
        $json = <<<'JSON'
            [
              {
                "Plan": {
                  "Node Type": "Seq Scan",
                  "Relation Name": "users",
                  "Startup Cost": 0.00,
                  "Total Cost": 35.50,
                  "Plan Rows": 100,
                  "Plan Width": 4,
                  "Actual Rows": 500,
                  "Actual Loops": 1
                }
              }
            ]
            JSON;

        $parser = new ExplainParser();
        $plan = $parser->parse($json);

        $accuracy = $plan->rootNode()->rowEstimateAccuracy();
        static::assertNotNull($accuracy);
        static::assertSame(5.0, $accuracy);
    }
}
