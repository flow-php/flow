<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Functions;

use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\RequiresPhp;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\html_element_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_html_element;

final class DOMElementParentTest extends FlowTestCase
{
    #[RequiresPhp('>= 8.4.0')]
    public function test_dom_element_value_from_dom_document(): void
    {
        static::assertSame(
            [
                [
                    'user_name' => 'User Name',
                    'user_id' => '01',
                ],
            ],
            df()
                ->read(from_rows(array_to_rows([[
                    'html_element' => type_html_element()->cast(
                        '<article><section><h1>User Name</h1></section><span>01</span></article>',
                    ),
                ]], schema(html_element_schema('html_element')))))
                ->withColumn('user_details', ref('html_element')->htmlQuerySelector('section'))
                ->withColumn('user_name', ref('user_details')->htmlQuerySelector('h1')->domElementValue())
                ->withColumn(
                    'user_id',
                    ref('user_details')->domElementParent()->htmlQuerySelector('span')->domElementValue(),
                )
                ->select('user_name', 'user_id')
                ->fetch()
                ->toArray(),
        );
    }
}
