<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Config;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Join\Join;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\Select;
use Flow\ETL\Plan\Node\Sort;
use Flow\ETL\Plan\Trigger;
use Flow\ETL\Row\References;
use Flow\ETL\Tests\Double\RepeatableExtractor;

use function array_values;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;

final class NodeMother
{
    public static function context(?Config $config = null): FlowContext
    {
        return flow_context($config ?? config());
    }

    public static function join(Node $left, Node $right): Node\Join
    {
        return new Node\Join($left, $right, join_on(['id' => 'id']), Join::left, null);
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

    public static function select(Node $input, string ...$entries): Select
    {
        return new Select($input, $entries === [] ? ['id'] : array_values($entries));
    }

    public static function sort(Node $input, ?References $refs = null): Sort
    {
        return new Sort($input, $refs ?? refs(ref('id')));
    }
}
