<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function\Structure;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use PHPUnit\Framework\TestCase;

final class StructureSelectTest extends TestCase
{
    public function test_structure_keep() : void
    {
        $rows = df()
            ->read(
                from_array(
                    [
                        [
                            'user' => [
                                'id' => 1,
                                'name' => 'username',
                                'email' => 'user_email@email.com',
                                'tags' => [
                                    'tag1',
                                    'tag2',
                                    'tag3',
                                ],
                            ],
                        ],
                    ]
                )
            )
            ->withEntry('user', ref('user')->structure()->select('id', 'email', 'tags'))
            ->fetch();

        $this->assertEquals(
            [
                [
                    'user' => [
                        'id' => 1,
                        'email' => 'user_email@email.com',
                        'tags' => [
                            'tag1',
                            'tag2',
                            'tag3',
                        ],
                    ],
                ],
            ],
            $rows->toArray()
        );
    }
}
