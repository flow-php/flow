<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Unit\Docs;

use Flow\Documentation\Docs\MarkdownFences;
use Flow\Documentation\Tests\Context\TempFileContext;
use PHPUnit\Framework\TestCase;

final class MarkdownFencesTest extends TestCase
{
    private TempFileContext $files;

    protected function setUp(): void
    {
        $this->files = new TempFileContext('md');
    }

    protected function tearDown(): void
    {
        $this->files->clean();
    }

    public function test_it_records_the_line_of_the_opening_fence(): void
    {
        $fences = (new MarkdownFences())->of($this->files->withContents("intro\n\n```php\n<?php echo 1;\n```\n"));

        static::assertSame(3, $fences[0]->line);
    }

    public function test_it_captures_the_body_without_the_fence_lines(): void
    {
        $fences = (new MarkdownFences())->of($this->files->withContents("```php\n<?php echo 1;\n```\n"));

        static::assertSame('<?php echo 1;', $fences[0]->code);
    }

    public function test_php_fences_are_selected_by_the_first_word_of_the_info_string(): void
    {
        $php = (new MarkdownFences())->phpOf($this->files->withContents(
            "```php \n<?php echo 1;\n```\n\n```text\nplain\n```\n",
        ));

        static::assertCount(1, $php);
        static::assertSame('php', $php[0]->info->language);
    }

    public function test_a_fence_keeps_its_ignore_modifier(): void
    {
        $php = (new MarkdownFences())->phpOf($this->files->withContents("```php ignore\n<?php echo\n```\n"));

        static::assertTrue($php[0]->info->isIgnored());
    }

    public function test_location_names_the_file_and_line(): void
    {
        $file = $this->files->withContents("```php\n<?php echo 1;\n```\n");

        static::assertSame($file . ':1', (new MarkdownFences())->of($file)[0]->location());
    }
}
