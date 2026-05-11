<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Unit\Output;

use Flow\Bridge\Symfony\HttpFoundation\Output\Type;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\Bridge\Symfony\HttpFoundation\http_xml_output;

final class XMLOutputTest extends FlowTestCase
{
    public function test_type(): void
    {
        static::assertSame(Type::XML, http_xml_output()->type());
    }
}
