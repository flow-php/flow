<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Unit\Attribute;

use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type;
use PHPUnit\Framework\TestCase;
use ReflectionFunction;

final class DocumentationDSLTest extends TestCase
{
    public function test_carries_module_and_type(): void
    {
        $dsl = (new ReflectionFunction(
            #[DocumentationDSL(module: Module::CORE, type: Type::HELPER)] static fn() => null,
        ))->getAttributes(DocumentationDSL::class)[0]->newInstance();

        static::assertSame(Module::CORE, $dsl->module);
        static::assertSame(Type::HELPER, $dsl->type);
    }
}
