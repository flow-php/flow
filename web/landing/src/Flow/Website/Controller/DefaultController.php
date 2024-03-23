<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Flow\Website\Service\{Examples, Github};
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class DefaultController extends AbstractController
{
    public function __construct(
        private readonly Github $github,
        private readonly Examples $examples,
    ) {
    }

    #[Route('/{topic}/{example}/', name: 'example')]
    public function example(string $topic, string $example) : Response
    {
        $topics = $this->examples->topics();
        $currentTopic = $topic;

        $examples = $this->examples->examples($currentTopic);
        $currentExample = $example;

        return $this->render('example/index.html.twig', [
            'topics' => $topics,
            'examples' => $examples,
            'currentTopic' => $topic,
            'currentExample' => $example,
            'code' => $this->examples->code($currentTopic, $currentExample),
            'output' => $this->examples->output($currentTopic, $currentExample),
        ]);
    }

    #[Route('/', name: 'main')]
    public function main() : Response
    {
        return $this->render('main/index.html.twig', [
            'topics' => $this->examples->topics(),
        ]);
    }

    #[Route('/{topic}/', name: 'topic', priority: 10)]
    public function topic(string $topic) : Response
    {
        $topics = $this->examples->topics();
        $currentTopic = $topic;

        $examples = $this->examples->examples($currentTopic);
        $currentExample = \current($examples);

        return $this->render('example/index.html.twig', [
            'topics' => $topics,
            'examples' => $examples,
            'currentTopic' => $currentTopic,
            'currentExample' => $currentExample,
            'code' => $this->examples->code($currentTopic, $currentExample),
            'output' => $this->examples->output($currentTopic, $currentExample),
        ]);
    }
}
