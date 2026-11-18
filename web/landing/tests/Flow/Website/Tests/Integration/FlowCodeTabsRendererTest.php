<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration;

use Flow\Website\Service\Markdown\FlowCodeTabsRenderer;
use League\CommonMark\CommonMarkConverter;
use League\CommonMark\Event\DocumentParsedEvent;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DomCrawler\Crawler;

final class FlowCodeTabsRendererTest extends TestCase
{
    public function render(string $markdown): Crawler
    {
        $converter = new CommonMarkConverter();
        $converter->getEnvironment()->addEventListener(DocumentParsedEvent::class, new FlowCodeTabsRenderer());

        return new Crawler('<main>' . $converter->convert($markdown)->getContent() . '</main>');
    }

    public function test_blocks_without_a_tab_name_stay_as_they_are(): void
    {
        static::assertCount(
            0,
            $this->render("```php\necho 1;\n```\n\n```php ignore\necho 2;\n```\n")->filter('.code-tabs'),
        );
    }

    public function test_neighboring_blocks_with_a_tab_name_become_one_group(): void
    {
        $group = $this->render(
            "```php ignore tab=\"Doctrine MySQL\"\necho 'mysql';\n```\n\n```php tab=\"Flow <PostgreSQL>\"\necho 'pgsql';\n```\n",
        )->filter('main > .code-tabs[data-controller="code-tabs"]');

        static::assertCount(1, $group);
        static::assertSame(
            ['Doctrine MySQL', 'Flow <PostgreSQL>'],
            $group
                ->filter(
                    '.code-tabs-list[role="tablist"] > button[role="tab"][data-code-tabs-target="tab"][data-action="code-tabs#select"]',
                )
                ->each(static fn(Crawler $tab): string => $tab->text()),
        );
        static::assertSame(
            ['Doctrine MySQL', 'Flow <PostgreSQL>'],
            $group
                ->filter('button[role="tab"]')
                ->each(static fn(Crawler $tab): ?string => $tab->attr('data-code-tabs-name-param')),
        );
        static::assertSame(
            ['Doctrine MySQL', 'Flow <PostgreSQL>'],
            $group
                ->filter('.code-tabs-panel[role="tabpanel"][data-code-tabs-target="panel"]')
                ->each(static fn(Crawler $panel): ?string => $panel->attr('data-tab')),
        );
        static::assertSame(
            ["echo 'mysql';\n", "echo 'pgsql';\n"],
            $group
                ->filter('.code-tabs-panel pre code')
                ->each(static fn(Crawler $code): string => $code->text(normalizeWhitespace: false)),
        );
        static::assertSame(
            ['Doctrine MySQL', 'Flow <PostgreSQL>'],
            $group
                ->filter('.code-tabs-panel > .code-tabs-label')
                ->each(static fn(Crawler $label): string => $label->text()),
        );
    }

    public function test_text_between_tab_blocks_starts_a_new_group(): void
    {
        $page = $this->render(
            "```sql tab=\"MySQL\"\nSELECT 1;\n```\n\n```sql tab=\"PostgreSQL\"\nSELECT 2;\n```\n\nText.\n\n```php tab=\"MySQL\"\necho 3;\n```\n",
        );

        static::assertSame(
            [2, 1],
            $page->filter('.code-tabs')->each(
                static fn(Crawler $group): int => $group->filter('.code-tabs-panel')->count(),
            ),
        );
    }
}
