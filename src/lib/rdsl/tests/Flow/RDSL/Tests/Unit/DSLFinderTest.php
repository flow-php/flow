<?php declare(strict_types=1);

namespace Flow\RDSL\Tests\Unit;

use Flow\RDSL\AccessControl\AllowAll;
use Flow\RDSL\AccessControl\DenyAll;
use Flow\RDSL\DSLNamespace;
use Flow\RDSL\Exception\InvalidArgumentException;
use Flow\RDSL\Finder;
use Flow\RDSL\Tests\Fixtures\IntObject;
use PHPUnit\Framework\TestCase;

require_once __DIR__ . '/../Fixtures/functions.php';

final class DSLFinderTest extends TestCase
{
    public function test_finding_functions_excluded_through_attributes() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Function \"exclude\" from namespace \"Flow\RDSL\Tests\Fixtures\" is excluded from DSL.");

        (new Finder(
            [new DSLNamespace('\Flow\RDSL\Tests\Fixtures', new AllowAll())],
            new AllowAll(),
            new AllowAll()
        ))
        ->findFunction('exclude', false);
    }

    public function test_finding_methods_excluded_through_attributes() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Method \"excluded\" from class \"Flow\RDSL\Tests\Fixtures\IntObject\" is excluded from DSL.");

        (new Finder(
            [new DSLNamespace('\Flow\RDSL\Tests\Fixtures', new AllowAll())],
            new AllowAll(),
            new AllowAll()
        ))
        ->findMethod(IntObject::class, 'excluded');
    }

    public function test_not_allowed_function_from_global_namespace_by_default() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Function "exec" from global namespace is not allowed to be executed.');

        (new Finder([], new AllowAll(), new AllowAll()))->findFunction('exec', false);
    }

    public function test_not_allowing_functions_from_namespaces_when_disallowed() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Function "int" from namespace "\Flow\RDSL\Tests\Fixtures" is not allowed to be executed.');

        (new Finder(
            [new DSLNamespace('\Flow\RDSL\Tests\Fixtures', new DenyAll())],
            new AllowAll(),
            new AllowAll()
        ))
            ->findFunction('int', false);
    }
}
