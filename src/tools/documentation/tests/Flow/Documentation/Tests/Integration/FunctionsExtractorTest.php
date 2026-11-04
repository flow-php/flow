<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Integration;

use Flow\Documentation\Attribute\Module;
use Flow\Documentation\FunctionsExtractor;
use PHPUnit\Framework\TestCase;
use Roave\BetterReflection\BetterReflection;

use function iterator_to_array;

final class FunctionsExtractorTest extends TestCase
{
    public function test_extract_functions_from_a_file(): void
    {
        $betterReflection = new BetterReflection();
        $functionsExtractor = new FunctionsExtractor(
            __DIR__,
            $betterReflection->reflector(),
            $betterReflection->astLocator(),
        );

        $functions = iterator_to_array($functionsExtractor->extract([__DIR__ . '/functions.php']));

        static::assertCount(2, $functions);
        static::assertSame([], $functions[1]->returnType->types);
        static::assertEquals(
            [
                'name' => 'doSomething',
                'namespace' => 'Flow\Documentation\Tests\Integration',
                'parameters' => [
                    [
                        'name' => 'argument',
                        'type' => [
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
                            [
                                'name' => 'DateTimeInterface',
                                'is_nullable' => false,
                                'is_variadic' => false,
                                'namespace' => '',
                            ],
                            [
                                'name' => 'ParameterClass',
                                'is_nullable' => false,
                                'is_variadic' => false,
                                'namespace' => 'Flow\Documentation\Tests\Integration\Double',
                            ],
                        ],
                        'has_default_value' => false,
                        'is_nullable' => false,
                        'is_variadic' => false,
                        'default_value' => null,
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
                            'module' => Module::CORE,
                            'limit' => 'PHP_INT_MAX',
                        ],
                    ],
                ],
                'scalar_function_chain' => false,
                'doc_comment' => null,
                'repository_path' => 'functions.php',
                'start_line_in_file' => 11,
                'slug' => 'dosomething',
            ],
            $functions[0]->normalize(),
        );
    }
}
