<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use Flow\Bridge\Symfony\HttpFoundation\Output\CSVOutput;
use Flow\Bridge\Symfony\HttpFoundation\Transformation\{MaskColumns};
use Flow\Bridge\Symfony\HttpFoundation\{FlowStreamedResponse};
use Flow\Website\Service\{Examples};
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class HomeController extends AbstractController
{
    public function __construct(
        private readonly Examples $examples,
    ) {
    }

    #[Route('/', name: 'home', options: ['sitemap' => true])]
    public function home() : Response
    {
        return $this->render('main/index.html.twig', [
            'topics' => $this->examples->topics(),
        ]);
    }

    #[Route('/export/report', name: 'export-report')]
    public function streamExample() : Response
    {
        return new FlowStreamedResponse(
            from_parquet(__DIR__ . '/reports/orders.parquet'),
            new CSVOutput(withHeader: true),
            new MaskColumns(['address'])
        );
    }
}
