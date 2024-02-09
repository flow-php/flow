<?php declare(strict_types=1);

namespace Flow\Website\Controller;

use Flow\Website\Service\Examples;
use Flow\Website\Service\Github;
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
            'contributors' => $this->github->contributors(),
            'topics' => $topics,
            'examples' => $examples,
            'currentTopic' => $topic,
            'currentExample' => $example,
            'code' => $this->examples->code($currentTopic, $currentExample),
        ]);
    }

    #[Route('/', name: 'main')]
    public function main() : Response
    {
        return $this->render('main/index.html.twig', [
            'contributors' => $this->github->contributors(),
            'topics' => $this->examples->topics(),
        ]);
    }

    #[Route('/{topic}/', name: 'topic')]
    public function topic(string $topic) : Response
    {
        $topics = $this->examples->topics();
        $currentTopic = $topic;

        $examples = $this->examples->examples($currentTopic);
        $currentExample = \current($examples);

        return $this->render('example/index.html.twig', [
            'contributors' => $this->github->contributors(),
            'topics' => $topics,
            'examples' => $examples,
            'currentTopic' => $currentTopic,
            'currentExample' => $currentExample,
            'code' => $this->examples->code($currentTopic, $currentExample),
        ]);
    }
}
