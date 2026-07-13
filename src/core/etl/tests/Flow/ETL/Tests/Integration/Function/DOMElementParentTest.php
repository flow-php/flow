<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\RequiresPhp;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\html_element_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class DOMElementParentTest extends FlowTestCase
{
    #[RequiresPhp('>= 8.4.0')]
    public function test_dom_element_value_from_dom_document(): void
    {
        $rows = df()
            ->read(from_rows(rows(row(html_element_entry(
                'html_element',
                '<article><section><h1>User Name</h1></section><span>01</span></article>',
            )))))
            ->withEntry('user_details', ref('html_element')->htmlQuerySelector('section'))
            ->withEntry('user_name', ref('user_details')->htmlQuerySelector('h1')->domElementValue())
            ->withEntry(
                'user_id',
                ref('user_details')->domElementParent()->htmlQuerySelector('span')->domElementValue(),
            )
            ->select('user_name', 'user_id')
            ->fetch();

        static::assertSame(
            [
                [
                    'user_name' => 'User Name',
                    'user_id' => '01',
                ],
            ],
            $rows->toArray(),
        );
    }
}
