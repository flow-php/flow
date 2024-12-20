<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use League\CommonMark\CommonMarkConverter;
use League\CommonMark\Extension\CommonMark\CommonMarkCoreExtension;
use League\CommonMark\Extension\CommonMark\Node\Block\FencedCode;
use League\CommonMark\Extension\ExternalLink\ExternalLinkExtension;

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
            ->addRenderer(FencedCode::class, new FlowCodeRenderer(), 0);
        //        $converter->getEnvironment()->addExtension(new CommonMarkCoreExtension());
        //        foreach ($this->extensions as $extension) {
        //            $converter->getEnvironment()->addRenderer(new FlowCodeRenderer());
        //        }

        return $converter;
    }
}
