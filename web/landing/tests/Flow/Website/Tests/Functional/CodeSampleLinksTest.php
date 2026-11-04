<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

final class CodeSampleLinksTest extends EndToEndTestCase
{
    private const string LINKS = '() => Object.fromEntries([...document.querySelectorAll("pre a[href]")].map(link => [link.textContent, link.getAttribute("href")]))';

    public function test_methods_link_to_the_api_documentation_of_the_resolved_class(): void
    {
        $browser = $this->playwrightBrowser()->visit('/documentation/components/core/corrupted-records.md');
        $page = $this->pageOf($browser);
        $page->waitForSelector('pre a[href]');

        $links = type_map(type_string(), type_string())->assert($page->evaluate(self::LINKS));

        static::assertSame(
            '/documentation/api/core/classes/Flow-ETL-API-DataFrame.html#method_withColumn',
            $links['withColumn'],
        );
        static::assertSame(
            '/documentation/api/core/classes/Flow-ETL-API-DataFrame.html#method_groupBy',
            $links['groupBy'],
        );
        static::assertSame(
            '/documentation/api/core/classes/Flow-ETL-API-DataFrame.html#method_filter',
            $links['filter'],
        );
        static::assertSame(
            '/documentation/api/core/classes/Flow-ETL-Functions-ScalarFunctionChain.html#method_greaterThan',
            $links['greaterThan'],
        );
        static::assertSame('/documentation/dsl/core/data-frame#dsl-function', $links['data_frame']);
    }
}
