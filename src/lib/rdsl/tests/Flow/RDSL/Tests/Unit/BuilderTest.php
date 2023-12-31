<?php declare(strict_types=1);

namespace Flow\RDSL\Tests\Unit;

use Flow\RDSL\AccessControl\AllowAll;
use Flow\RDSL\AccessControl\AllowList;
use Flow\RDSL\Builder;
use Flow\RDSL\DSLNamespace;
use Flow\RDSL\Exception\InvalidArgumentException;
use Flow\RDSL\Executable;
use Flow\RDSL\Finder;
use PHPUnit\Framework\TestCase;

final class BuilderTest extends TestCase
{
    public function test_parsing_array_with_function_defined_as_not_string() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Definition must start with a function: {"function":"name","args":[]}');

        $builder = new Builder(new Finder([], new AllowAll(), new AllowAll()));

        $builder->parse(['function' => []]);
    }

    public function test_parsing_array_with_function_defined_as_string() : void
    {
        $builder = new Builder(new Finder(
            [DSLNamespace::global(new AllowList(['strlen']))],
            new AllowAll(),
            new AllowAll()
        ));

        $executables = $builder->parse([['function' => 'strlen', 'args' => ['string']]]);

        $this->assertInstanceOf(Executable::class, $executables->toArray()[0]);
        $this->assertSame('strlen', $executables->toArray()[0]->name());
        $this->assertSame(['string'], $executables->toArray()[0]->arguments()->toArray());
    }

    public function test_parsing_array_with_non_existing_function() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Function "non_existing_function" does not exist');

        $builder = new Builder(new Finder([], new AllowAll(), new AllowAll()));

        $builder->parse([['function' => 'non_existing_function', 'args' => ['string']]]);
    }

    public function test_parsing_array_without_function_defined() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Definition must have at least one function: [{"function":"name","args":[]}]');

        $builder = new Builder(new Finder([], new AllowAll(), new AllowAll()));

        $builder->parse([]);
    }
}
