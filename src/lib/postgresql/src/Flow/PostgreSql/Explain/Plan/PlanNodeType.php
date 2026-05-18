<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain\Plan;

use function in_array;
use function strcasecmp;

enum PlanNodeType: string
{
    case AGGREGATE = 'Aggregate';
    case APPEND = 'Append';
    case BITMAP_HEAP_SCAN = 'Bitmap Heap Scan';
    case BITMAP_INDEX_SCAN = 'Bitmap Index Scan';
    case CTE_SCAN = 'CTE Scan';
    case CUSTOM_SCAN = 'Custom Scan';
    case FOREIGN_SCAN = 'Foreign Scan';
    case FUNCTION_SCAN = 'Function Scan';
    case GATHER = 'Gather';
    case GATHER_MERGE = 'Gather Merge';
    case GROUP_AGGREGATE = 'GroupAggregate';
    case HASH = 'Hash';
    case HASH_AGGREGATE = 'HashAggregate';
    case HASH_JOIN = 'Hash Join';
    case INCREMENTAL_SORT = 'Incremental Sort';
    case INDEX_ONLY_SCAN = 'Index Only Scan';
    case INDEX_SCAN = 'Index Scan';
    case LIMIT = 'Limit';
    case LOCK_ROWS = 'LockRows';
    case MATERIALIZE = 'Materialize';
    case MEMOIZE = 'Memoize';
    case MERGE_APPEND = 'Merge Append';
    case MERGE_JOIN = 'Merge Join';
    case MIXED_AGGREGATE = 'MixedAggregate';
    case MODIFY_TABLE = 'ModifyTable';
    case NESTED_LOOP = 'Nested Loop';
    case PROJECT_SET = 'ProjectSet';
    case RECURSIVE_UNION = 'Recursive Union';
    case RESULT = 'Result';
    case SAMPLE_SCAN = 'Sample Scan';
    case SEQ_SCAN = 'Seq Scan';
    case SET_OP = 'SetOp';
    case SORT = 'Sort';
    case SUBQUERY_SCAN = 'Subquery Scan';
    case TID_SCAN = 'Tid Scan';
    case UNIQUE = 'Unique';
    case UNKNOWN = 'Unknown';
    case VALUES_SCAN = 'Values Scan';
    case WINDOW_AGG = 'WindowAgg';
    case WORK_TABLE_SCAN = 'WorkTable Scan';

    public static function fromString(string $nodeType): self
    {
        foreach (self::cases() as $case) {
            if (strcasecmp($case->value, $nodeType) === 0) {
                return $case;
            }
        }

        return self::UNKNOWN;
    }

    public function isAggregate(): bool
    {
        return in_array(
            $this,
            [
                self::AGGREGATE,
                self::GROUP_AGGREGATE,
                self::HASH_AGGREGATE,
                self::MIXED_AGGREGATE,
            ],
            true,
        );
    }

    public function isJoin(): bool
    {
        return in_array(
            $this,
            [
                self::NESTED_LOOP,
                self::MERGE_JOIN,
                self::HASH_JOIN,
            ],
            true,
        );
    }

    public function isScan(): bool
    {
        return in_array(
            $this,
            [
                self::SEQ_SCAN,
                self::INDEX_SCAN,
                self::INDEX_ONLY_SCAN,
                self::BITMAP_HEAP_SCAN,
                self::BITMAP_INDEX_SCAN,
                self::TID_SCAN,
                self::SUBQUERY_SCAN,
                self::FUNCTION_SCAN,
                self::VALUES_SCAN,
                self::CTE_SCAN,
                self::WORK_TABLE_SCAN,
                self::FOREIGN_SCAN,
                self::CUSTOM_SCAN,
                self::SAMPLE_SCAN,
            ],
            true,
        );
    }

    public function isSort(): bool
    {
        return in_array(
            $this,
            [
                self::SORT,
                self::INCREMENTAL_SORT,
            ],
            true,
        );
    }
}
