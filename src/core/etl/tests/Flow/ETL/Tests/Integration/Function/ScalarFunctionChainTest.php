<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\all;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use PHPUnit\Framework\TestCase;

final class ScalarFunctionChainTest extends TestCase
{
    public function test_function_chain_on_a_single_element() : void
    {
        $this->assertEquals(
            all(ref('id')->equals(lit(1))),
            all(ref('id')->equals(lit(1))),
        );
    }

    public function test_function_chain_root() : void
    {
        $this->assertEquals(
            ref('id'),
            ref('id')->plus(lit(10))->minus(lit(2))->multiply(lit(3))->getRootFunction()
        );
    }
}
