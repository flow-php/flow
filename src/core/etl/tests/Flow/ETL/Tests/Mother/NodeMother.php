<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\IO\Extractor;
use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Limit;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Logical\Node\Select;
use Flow\ETL\Planning\Logical\Node\Sort;
use Flow\ETL\Planning\Logical\Trigger;
use Flow\ETL\Runtime\Config;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Tests\Double\RepeatableExtractor;
use Flow\ETL\Values\Join\JoinType;

use function array_values;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\ref;

final class NodeMother
{
    public static function context(?Config $config = null): FlowContext
    {
        return flow_context($config ?? config());
    }

    public static function join(Node $left, Node $right): Node\Join
    {
        return new Node\Join($left, $right, join_on(['id' => 'id']), JoinType::left, null);
    }

    public static function crossJoin(Node $left, Node $right): Node\CrossJoin
    {
        return new Node\CrossJoin($left, $right, 'r_');
    }

    public static function nonRepeatableRead(): Read
    {
        return new Read(new RepeatableExtractor(false));
    }

    public static function joinRight(LogicalPlan $plan): Node
    {
        return $plan->root;
    }

    public static function limit(Node $input, int $limit): Limit
    {
        return new Limit($input, $limit);
    }

    public static function plan(Node $root): LogicalPlan
    {
        return Trigger::rows->plan($root);
    }

    public static function read(?Extractor $extractor = null): Read
    {
        return new Read($extractor ?? from_array([['id' => 1]]));
    }

    public static function select(Node $input, string ...$columns): Select
    {
        return new Select($input, $columns === [] ? ['id'] : array_values($columns));
    }

    /**
     * @param null|list<SortColumn> $columns
     */
    public static function sort(Node $input, ?array $columns = null): Sort
    {
        return new Sort($input, $columns ?? [ref('id')->asc()]);
    }
}
