<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\Service\Markdown;

use Flow\Website\Service\Markdown\FlowCodeRenderer;
use Flow\Website\Tests\Double\NullChildNodeRenderer;
use InvalidArgumentException;
use League\CommonMark\Extension\CommonMark\Node\Block\FencedCode;
use League\CommonMark\Node\Block\Paragraph;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Asset\Packages;
use Symfony\Component\Asset\PathPackage;
use Symfony\Component\Asset\VersionStrategy\EmptyVersionStrategy;
use Symfony\Component\DomCrawler\Crawler;

use function Flow\Types\DSL\type_string;

final class FlowCodeRendererTest extends TestCase
{
    public function render(string $code, string $language = ''): Crawler
    {
        $node = new FencedCode(3, '`', 0);
        $node->setInfo($language);
        $node->setLiteral($code);

        return new Crawler((string) (new FlowCodeRenderer(new Packages(
            new PathPackage('/', new EmptyVersionStrategy()),
        )))->render($node, new NullChildNodeRenderer()));
    }

    public function test_code_block_has_a_copy_button_for_its_code(): void
    {
        $block = $this->render("<?php\n\necho 'a < b';\n", 'php');
        $button = $block->filter('div.relative > button[data-controller="clipboard"]');
        $highlighted = $block->filter('div.relative > pre.language-php[data-controller="syntax-highlight"]');

        static::assertCount(1, $button);
        static::assertCount(1, $highlighted);
        static::assertCount(1, $highlighted->filter('code.language-php'));
        static::assertSame('documentation', $button->attr('data-clipboard-kind-value'));
        static::assertSame('Copy code', $button->attr('aria-label'));
        static::assertMatchesRegularExpression(
            '/^#code-[0-9a-f]{12}$/',
            type_string()->assert($button->attr('data-clipboard-target')),
        );
        static::assertSame(
            $button->attr('data-clipboard-target'),
            '#' . type_string()->assert($highlighted->attr('id')),
        );
        static::assertSame('/images/icons/copy.svg', $button->filter('img')->attr('src'));
        static::assertSame("<?php\n\necho 'a < b';\n", $highlighted->text(normalizeWhitespace: false));
    }

    public function test_code_without_a_language_is_plain(): void
    {
        static::assertCount(1, $this->render('plain')->filter('pre.language-plain > code.language-plain'));
    }

    public function test_identical_blocks_get_the_same_id(): void
    {
        static::assertSame(
            $this->render('echo 1;')->filter('pre')->attr('id'),
            $this->render('echo 1;')->filter('pre')->attr('id'),
        );
        static::assertNotSame(
            $this->render('echo 1;')->filter('pre')->attr('id'),
            $this->render('echo 2;')->filter('pre')->attr('id'),
        );
    }

    public function test_throws_on_incompatible_node_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Incompatible node type: ' . Paragraph::class);

        (new FlowCodeRenderer(new Packages(new PathPackage('/', new EmptyVersionStrategy()))))->render(
            new Paragraph(),
            new NullChildNodeRenderer(),
        );
    }
}
