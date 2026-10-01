<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Window\PeerComparator;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_xml_element;

use const NAN;

final class PeerComparatorTest extends FlowTestCase
{
    public function test_rows_are_peers_when_every_order_value_is_equal(): void
    {
        $partition = array_to_rows(
            [
                ['at' => new DateTimeImmutable('2024-01-01 01:00:00', new DateTimeZone('+01:00')), 'n' => 1],
                ['at' => new DateTimeImmutable('2024-01-01 00:00:00', new DateTimeZone('UTC')), 'n' => 1],
                ['at' => new DateTimeImmutable('2024-01-01 00:00:00', new DateTimeZone('UTC')), 'n' => 2],
            ],
            schema(datetime_schema('at'), int_schema('n')),
        );
        static::assertSame(
            [false, true, false],
            (new PeerComparator([ref('at'), ref('n')]))->peersOfPrevious($partition),
        );
    }

    public function test_nan_is_a_peer_of_nan(): void
    {
        $partition = array_to_rows([['v' => NAN], ['v' => NAN], ['v' => 1.0]], schema(float_schema('v')));
        static::assertSame([false, true, false], (new PeerComparator([ref('v')]))->peersOfPrevious($partition));
    }

    public function test_peers_of_previous_marks_each_row_against_the_row_before_it(): void
    {
        $partition = array_to_rows(
            [
                ['at' => new DateTimeImmutable('2024-01-01 01:00:00', new DateTimeZone('+01:00')), 'n' => 1],
                ['at' => new DateTimeImmutable('2024-01-01 00:00:00', new DateTimeZone('UTC')), 'n' => 1],
                ['at' => new DateTimeImmutable('2024-01-01 00:00:00', new DateTimeZone('UTC')), 'n' => 2],
            ],
            schema(datetime_schema('at'), int_schema('n')),
        );

        static::assertSame(
            [false, true, false],
            (new PeerComparator([ref('at'), ref('n')]))->peersOfPrevious($partition),
        );
    }

    public function test_peers_of_previous_treats_nan_as_a_peer_of_nan(): void
    {
        static::assertSame(
            [false, true, false],
            (new PeerComparator([ref('v')]))->peersOfPrevious(array_to_rows([
                ['v' => NAN],
                ['v' => NAN],
                ['v' => 1.0],
            ], schema(float_schema('v')))),
        );
        static::assertSame(
            [],
            (new PeerComparator([ref('v')]))->peersOfPrevious(array_to_rows([], schema(float_schema('v')))),
        );
    }

    public function test_peers_of_previous_compares_values_without_a_physical_order(): void
    {
        static::assertSame(
            [false, true, false],
            (new PeerComparator([ref('l')]))->peersOfPrevious(array_to_rows([
                ['l' => [1, 2]],
                ['l' => [1, 2]],
                ['l' => [2]],
            ], schema(list_schema('l', type_list(type_integer()))))),
        );
    }

    public function test_xml_elements_of_different_documents_are_peers_by_their_markup(): void
    {
        static::assertSame(
            [false, true, false],
            (new PeerComparator([ref('e')]))->peersOfPrevious(array_to_rows([
                ['e' => type_xml_element()->cast('<div><p>a</p></div>')->firstElementChild],
                ['e' => type_xml_element()->cast('<section><p>a</p></section>')->firstElementChild],
                ['e' => type_xml_element()->cast('<div><p>b</p></div>')->firstElementChild],
            ], schema(xml_element_schema('e')))),
        );
    }
}
