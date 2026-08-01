<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

use function file_get_contents;
use function Flow\Types\DSL\type_string;

final class ShopController extends AbstractController
{
    /**
     * @param array<string, string> $checkoutLinks
     */
    public function __construct(
        private readonly array $checkoutLinks,
    ) {}

    #[Route('/work-shop', name: 'shop', options: ['sitemap' => false])]
    public function index(): Response
    {
        return $this->render('shop/index.html.twig');
    }

    #[Route('/work-shop/blueprints/symfony-backoffice', name: 'shop_blueprint_symfony', options: ['sitemap' => false])]
    public function blueprintSymfony(): Response
    {
        return $this->render('shop/blueprints/symfony-backoffice/index.html.twig', [
            'listing_checkout_url' => $this->checkoutLinks['symfony_backoffice'] ?? null,
        ]);
    }

    #[Route('/work-shop/blueprints/how-it-works', name: 'shop_blueprint_how_it_works', options: ['sitemap' => false])]
    public function blueprintHowItWorks(): Response
    {
        return $this->render('shop/blueprints/how-it-works/index.html.twig', [
            'listing_checkout_url' => $this->checkoutLinks['symfony_backoffice'] ?? null,
        ]);
    }

    #[Route('/work-shop/success', name: 'shop_success', options: ['sitemap' => false])]
    public function success(): Response
    {
        return $this->render('shop/success/index.html.twig');
    }

    #[Route('/work-shop/consulting', name: 'shop_consulting', options: ['sitemap' => false])]
    public function consulting(): Response
    {
        return $this->render('shop/consulting/index.html.twig');
    }

    #[Route('/work-shop/terms-of-sales', name: 'shop_terms_of_sales', options: ['sitemap' => false])]
    public function termsOfSales(): Response
    {
        return $this->render('shop/terms-of-sales/index.html.twig', [
            'terms_markdown' => file_get_contents(
                type_string()->assert($this->getParameter('kernel.project_dir')) . '/content/shop/terms-of-sales.md',
            ),
        ]);
    }

    #[Route('/work-shop/privacy-policy', name: 'shop_privacy_policy', options: ['sitemap' => false])]
    public function privacyPolicy(): Response
    {
        return $this->render('shop/privacy-policy/index.html.twig', [
            'privacy_markdown' => file_get_contents(
                type_string()->assert($this->getParameter('kernel.project_dir')) . '/content/shop/privacy-policy.md',
            ),
        ]);
    }
}
