<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Unit\Docs;

use Flow\Documentation\Docs\DocumentationLinks;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class DocumentationLinksTest extends TestCase
{
    #[TestWith(['/documentation/components/core/partitioning.md', true])]
    #[TestWith(['/documentation/components/core/partitioning', true])]
    #[TestWith(['/documentation/components/core/there-is-no-such-page', false])]
    #[TestWith(['https://flow-php.com/documentation/components/core/partitioning', true])]
    #[TestWith(['https://flow-php.com/documentation/components/core/dataframe/window-functions', false])]
    public function test_a_page_link_resolves_against_the_documentation_tree(string $href, bool $resolves): void
    {
        static::assertSame($resolves, (new DocumentationLinks())->resolves($href));
    }

    #[TestWith(['/documentation/api/core'])]
    #[TestWith(['/documentation/dsl/core'])]
    #[TestWith(['/documentation/examples'])]
    #[TestWith(['/documentation'])]
    public function test_a_generated_route_resolves_without_a_file(string $href): void
    {
        static::assertTrue((new DocumentationLinks())->resolves($href));
    }

    #[TestWith(['/documentation/components/core/partitioning', true])]
    #[TestWith(['https://flow-php.com/documentation/a', true])]
    #[TestWith(['https://github.com/flow-php/flow', false])]
    #[TestWith(['/src/core/etl/composer.json', false])]
    public function test_only_documentation_hrefs_are_collected(string $href, bool $isDocumentation): void
    {
        static::assertSame($isDocumentation, (new DocumentationLinks())->isDocumentationHref($href));
    }

    public function test_an_anchor_does_not_stop_a_link_resolving(): void
    {
        static::assertTrue((new DocumentationLinks())->resolves(
            '/documentation/components/core/partitioning#overwrite',
        ));
    }
}
