<?php declare(strict_types=1);

namespace Flow\RDSL\Tests\Integration;

use Flow\RDSL\AccessControl\AllowList;
use Flow\RDSL\Builder;
use Flow\RDSL\DSLNamespace;
use Flow\RDSL\Executor;
use Flow\RDSL\Finder;
use Flow\RDSL\Tests\Fixtures\IntObject;
use PHPUnit\Framework\TestCase;

require_once __DIR__ . '/../Fixtures/functions.php';

final class ExecutorTest extends TestCase
{
    public function test_build_and_execute_dsl() : void
    {
        $builder = new Builder(new Finder([
            new DSLNamespace('\Flow\RDSL\Tests\Fixtures'),
        ], new AllowList(['int'])));

        $executables = $builder->parse(
            [
                [
                    'function' => 'int',
                    'args' => [0],
                    'call' => [
                        'method' => 'add',
                        'args' => [
                            [
                                'function' => 'lit',
                                'args' => [5],
                            ],
                        ],
                    ],
                ],
            ]
        );

        $results = (new Executor())->execute($executables);

        $this->assertInstanceOf(IntObject::class, $results[0]);
        $this->assertSame(5, $results[0]->value());
    }
}
