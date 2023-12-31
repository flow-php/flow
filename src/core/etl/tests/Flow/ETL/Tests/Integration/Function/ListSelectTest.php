<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\list_ref;
use PHPUnit\Framework\TestCase;

final class ListSelectTest extends TestCase
{
    public function test_selecting_properties_from_list() : void
    {
        $rows = df()
            ->read(
                from_array([
                    ['list' => [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test2'], ['id' => 3, 'name' => 'test3']]],
                    ['list' => [['id' => 4, 'name' => 'test4'], ['id' => 5, 'name' => 'test5'], ['id' => 6, 'name' => 'test6']]],
                ])
            )
            ->withEntry('list', list_ref('list')->select('id'))
            ->fetch();

        $this->assertEquals(
            [
                ['list' => [['id' => 1], ['id' => 2], ['id' => 3]]],
                ['list' => [['id' => 4], ['id' => 5], ['id' => 6]]],
            ],
            $rows->toArray()
        );
    }
}
