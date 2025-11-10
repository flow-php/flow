<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_rows, html_element_entry, ref, row, rows};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\RequiresPhp;

final class DOMElementParentTest extends FlowTestCase
{
    #[RequiresPhp('>= 8.4')]
    public function test_dom_element_value_from_dom_document() : void
    {
        $rows = df()
            ->read(from_rows(
                rows(
                    row(
                        html_element_entry('html_element', '<article><section><h1>User Name</h1></section><span>01</span></article>')
                    )
                )
            ))
            ->withEntry('user_details', ref('html_element')->htmlQuerySelector('section'))
            ->withEntry('user_name', ref('user_details')->htmlQuerySelector('h1')->domElementValue())
            ->withEntry('user_id', ref('user_details')->domElementParent()->htmlQuerySelector('span')->domElementValue())
            ->select('user_name', 'user_id')
            ->fetch();

        self::assertSame(
            [
                [
                    'user_name' => 'User Name',
                    'user_id' => '01',
                ],
            ],
            $rows->toArray()
        );
    }
}
