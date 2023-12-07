<?php declare(strict_types=1);

namespace Flow\RDSL\Tests\Unit;

use Flow\RDSL\AccessControl\AllowAll;
use Flow\RDSL\AccessControl\DenyAll;
use Flow\RDSL\DSLNamespace;
use Flow\RDSL\Exception\InvalidArgumentException;
use Flow\RDSL\Finder;
use PHPUnit\Framework\TestCase;

final class DSLFinderTest extends TestCase
{
    public function test_not_allowed_function_from_global_namespace_by_default() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Function "exec" from global namespace is not allowed to be executed.');

        (new Finder([], new AllowAll()))->findFunction('exec', false);
    }

    public function test_not_allowing_functions_from_namespaces_when_disallowed() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Function "int" from namespace "\Flow\RDSL\Tests\Fixtures" is not allowed to be executed.');

        (new Finder([new DSLNamespace('\Flow\RDSL\Tests\Fixtures', new DenyAll())], new AllowAll()))
            ->findFunction('int', false);
    }
}
