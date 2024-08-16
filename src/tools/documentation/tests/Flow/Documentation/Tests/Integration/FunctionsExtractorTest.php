<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Integration;

use Flow\Documentation\{FunctionCollector, FunctionsExtractor};
use PHPUnit\Framework\TestCase;

final class FunctionsExtractorTest extends TestCase
{
    public function test_extract_functions_from_a_file() : void
    {
        $functionsExtractor = new FunctionsExtractor(
            __DIR__,
            new FunctionCollector()
        );

        $functions = \iterator_to_array($functionsExtractor->extract([__DIR__ . '/functions.php']));

        self::assertCount(1, $functions);
        self::assertEquals(
            [
                'name' => 'doSomething',
                'namespace' => 'Flow\Documentation\Tests\Integration',
                'parameters' => [
                    [
                        'name' => 'argument',
                        'type' => [
                            [
                                'name' => \DateTimeInterface::class,
                                'is_nullable' => false,
                                'is_variadic' => false,
                                'namespace' => null,
                            ],
                            [
                                'name' => 'ParameterClass',
                                'is_nullable' => false,
                                'is_variadic' => false,
                                'namespace' => 'Flow\Documentation\Tests\Integration\Double',
                            ],
                            [
                                'name' => 'string',
                                'is_nullable' => false,
                                'is_variadic' => false,
                                'namespace' => null,
                            ],
                            [
                                'name' => 'int',
                                'is_nullable' => false,
                                'is_variadic' => false,
                                'namespace' => null,
                            ],
                            [
                                'name' => 'float',
                                'is_nullable' => false,
                                'is_variadic' => false,
                                'namespace' => null,
                            ],
                        ],
                        'has_default_value' => false,
                        'is_nullable' => false,
                        'is_variadic' => false,
                    ],
                ],
                'return_type' => [
                    [
                        'name' => 'bool',
                        'is_nullable' => true,
                        'is_variadic' => false,
                        'namespace' => null,
                    ],
                ],
                'attributes' => [
                    [
                        'name' => 'TestAttribute',
                        'namespace' => 'Flow\Documentation\Tests\Integration',
                        'arguments' => [
                            'name' => 'test',
                            'active' => true,
                        ],
                    ],
                ],
                'doc_comment' => null,
                'repository_path' => 'functions.php',
                'start_line_in_file' => 10,
                'slug' => 'dosomething',
            ],
            $functions[0]->normalize()
        );

    }
}
