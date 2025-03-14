<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Unit\Output;

use function Flow\Bridge\Symfony\HttpFoundation\{http_xml_output};
use Flow\Bridge\Symfony\HttpFoundation\Output\Type;
use Flow\ETL\Tests\FlowTestCase;

final class XMLOutputTest extends FlowTestCase
{
    public function test_type() : void
    {
        self::assertSame(Type::XML, http_xml_output()->type());
    }
}
