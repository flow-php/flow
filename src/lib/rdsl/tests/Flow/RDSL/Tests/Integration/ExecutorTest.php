<?php

declare(strict_types=1);

namespace Flow\RDSL\Tests\Integration;

use Flow\RDSL\AccessControl\{AllowAll, AllowList, Except};
use Flow\RDSL\Tests\Fixtures\IntObject;
use Flow\RDSL\{Builder, DSLNamespace, Executor, Finder};
use PHPUnit\Framework\TestCase;

require_once __DIR__ . '/../Fixtures/functions.php';

final class ExecutorTest extends TestCase
{
    public function test_build_and_execute_dsl() : void
    {
        $builder = new Builder(new Finder(
            [new DSLNamespace('\Flow\RDSL\Tests\Fixtures')],
            new AllowList(['int']),
            new AllowAll()
        ));

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

        self::assertInstanceOf(IntObject::class, $results[0]);
        self::assertSame(5, $results[0]->value());
    }

    public function test_executing_not_allowed_methods() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Method "Flow\RDSL\Tests\Fixtures\IntObject::add" is not allowed to be executed.');

        $builder = new Builder(new Finder(
            [new DSLNamespace('\Flow\RDSL\Tests\Fixtures')],
            new AllowList(['int']),
            new Except(new AllowAll(), [IntObject::class . '::add'])
        ));

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

        (new Executor())->execute($executables);
    }
}
