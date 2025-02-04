<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use League\CommonMark\CommonMarkConverter;
use League\CommonMark\Extension\CommonMark\Node\Block\FencedCode;
use League\CommonMark\Extension\CommonMark\Node\Inline\Link;
use League\CommonMark\Extension\ExternalLink\ExternalLinkExtension;
use League\CommonMark\Extension\FrontMatter\FrontMatterExtension;
use League\CommonMark\Extension\HeadingPermalink\{HeadingPermalinkExtension};
use League\CommonMark\Extension\Mention\MentionExtension;
use League\CommonMark\Extension\Table\TableExtension;

final class LeagueCommonMarkConverterFactory
{
    public function __invoke() : CommonMarkConverter
    {
        $config = [
            'external_link' => [
                'open_in_new_window' => true,
                'noreferrer' => 'all',
            ],
            'mentions' => [
                'github_handle' => [
                    'prefix' => '@',
                    'pattern' => '[a-z\d](?:[a-z\d]|-(?=[a-z\d])){0,38}(?!\w)',
                    'generator' => 'https://github.com/%s',
                ],
                'github_issue' => [
                    'prefix' => '#',
                    'pattern' => '\d+',
                    'generator' => 'https://github.com/flow-php/flow/issues/%d',
                ],
            ],
            'heading_permalink' => [
                'html_class' => 'mr-2',
                'id_prefix' => 'flow-php',
                'insert' => 'before',
                'title' => 'Permalink',
                'symbol' => '#',
            ],
        ];

        $converter = new CommonMarkConverter($config);

        $converter->getEnvironment()
            ->addExtension(new HeadingPermalinkExtension())
            ->addExtension(new ExternalLinkExtension())
            ->addExtension(new FrontMatterExtension())
            ->addExtension(new MentionExtension())
            ->addExtension(new TableExtension())
            ->addRenderer(FencedCode::class, new FlowCodeRenderer(), 0)
            ->addRenderer(Link::class, new FlowLinkRenderer(), 0);

        return $converter;
    }
}
