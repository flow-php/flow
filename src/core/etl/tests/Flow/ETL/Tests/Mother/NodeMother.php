<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Config;
use Flow\ETL\DataFrame;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Frame;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Select;
use Flow\ETL\Plan\Node\Sort;
use Flow\ETL\Plan\Snapshot;
use Flow\ETL\Row\References;
use Flow\ETL\Tests\Double\RecordingScanExtractor;

use function array_values;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;

final class NodeMother
{
    public static function context(?Config $config = null): FlowContext
    {
        return flow_context($config ?? config());
    }

    public static function frame(LogicalPlan $plan, ?FlowContext $context = null): Frame
    {
        return new Frame(new Snapshot($plan, $context ?? self::context()));
    }

    public static function frameOf(DataFrame $dataFrame): Frame
    {
        return new Frame($dataFrame->snapshot());
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

    public static function scannableRead(): Read
    {
        return new Read(new RecordingScanExtractor(schema(int_schema('id'))));
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
