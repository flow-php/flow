<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Functions\Window\WindowFrame;
use Flow\ETL\Functions\WindowFunction;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_instance_of;
use function range;
use function var_export;

final class AggregatorRoundTrip
{
    /**
     * value() of $aggregator and of its described-and-rebuilt copy over the 6-row fixture, as var_export strings.
     *
     * @return array{string, string}
     */
    public static function aggregated(AggregatingFunction $aggregator): array
    {
        $rebuilt = DescribeRoundTrip::rebuilt($aggregator);
        $rows = AggregatorFixtures::rows(new PhpBackend());

        $resolved = type_instance_of(AggregatingFunction::class)->assert((new ReferenceResolver())->resolve(
            $aggregator,
            AggregatorFixtures::schema(),
        ));
        $resolved->aggregate($rows, range(0, 5), flow_context()->backend());
        $copy = type_instance_of(AggregatingFunction::class)->assert((new ReferenceResolver())->resolve(
            $rebuilt,
            AggregatorFixtures::schema(),
        ));
        $copy->aggregate($rows, range(0, 5), flow_context()->backend());

        return [var_export($resolved->value(), true), var_export($copy->value(), true)];
    }

    /**
     * bounds() of $frame and of its rebuilt copy at every index of the partition.
     *
     * @return array{list<array{int, int}>, list<array{int, int}>}
     */
    public static function bounds(WindowFrame $frame): array
    {
        $partition = self::partition();
        $rebuilt = DescribeRoundTrip::rebuilt($frame);
        $original = [];
        $copy = [];

        foreach (range(0, $partition->count() - 1) as $index) {
            $original[] = $frame->bounds($index, $partition);
            $copy[] = $rebuilt->bounds($index, $partition);
        }

        return [$original, $copy];
    }

    public static function partition(): Rows
    {
        return array_to_rows(
            [['d' => 1, 'v' => 10], ['d' => 1, 'v' => 20], ['d' => 2, 'v' => 20], ['d' => 3, 'v' => 30]],
            schema(int_schema('d'), int_schema('v')),
        );
    }

    /**
     * values() of $function and of its rebuilt copy over a 4-row partition.
     *
     * @return array{list<mixed>, list<mixed>}
     */
    public static function windowed(WindowFunction $function): array
    {
        return [
            $function->values(self::partition(), flow_context()->backend()),
            DescribeRoundTrip::rebuilt($function)->values(self::partition(), flow_context()->backend()),
        ];
    }
}
