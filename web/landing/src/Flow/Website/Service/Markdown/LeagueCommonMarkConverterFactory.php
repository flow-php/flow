<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use League\CommonMark\CommonMarkConverter;
use League\CommonMark\Extension\CommonMark\Node\Block\FencedCode;
use League\CommonMark\Extension\CommonMark\Node\Inline\Link;
use League\CommonMark\Extension\ExternalLink\ExternalLinkExtension;
use League\CommonMark\Extension\FrontMatter\FrontMatterExtension;

final class LeagueCommonMarkConverterFactory
{
    public function __invoke() : CommonMarkConverter
    {
        $config = [
            'external_link' => [
                'open_in_new_window' => true,
                'noreferrer' => 'all',
            ],
        ];

        $converter = new CommonMarkConverter($config);

        $converter->getEnvironment()
            ->addExtension(new ExternalLinkExtension())
            ->addExtension(new FrontMatterExtension())
            ->addRenderer(FencedCode::class, new FlowCodeRenderer(), 0)
            ->addRenderer(Link::class, new FlowLinkRenderer(), 0);

        return $converter;
    }
}
