<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Config;
use Flow\ETL\DataFrame;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Join\Join;
use Flow\ETL\Plan;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Select;
use Flow\ETL\Plan\Node\SideInput;
use Flow\ETL\Plan\Node\Sort;
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

    public static function frame(LogicalPlan $plan, ?FlowContext $context = null): SideInput
    {
        return new SideInput(self::framePlan($plan, $context));
    }

    public static function frameOf(DataFrame $dataFrame): SideInput
    {
        return new SideInput($dataFrame->explain());
    }

    public static function join(Node $left, SideInput $right): Node\Join
    {
        return new Node\Join($left, $right, join_on(['id' => 'id']), Join::left, null);
    }

    public static function crossJoin(Node $left, SideInput $right): Node\CrossJoin
    {
        return new Node\CrossJoin($left, $right, 'r_');
    }

    public static function nonRepeatableRead(): Read
    {
        return new Read(new RepeatableExtractor(false));
    }

    public static function limit(Node $input, int $limit): Limit
    {
        return new Limit($input, $limit);
    }

    public static function plan(Node $root): LogicalPlan
    {
        return new LogicalPlan(new Result($root));
    }

    public static function read(?Extractor $extractor = null): Read
    {
        return new Read($extractor ?? from_array([['id' => 1]]));
    }

    public static function select(Node $input, string ...$entries): Select
    {
        return new Select($input, $entries === [] ? ['id'] : array_values($entries));
    }

    public static function framePlan(LogicalPlan $plan, ?FlowContext $context = null): Plan
    {
        return Plan::of($plan, $context ?? self::context());
    }

    public static function sort(Node $input, ?References $refs = null): Sort
    {
        return new Sort($input, $refs ?? refs(ref('id')));
    }
}
