<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use Flow\Website\Kernel;
use Flow\Website\Playwright\BinarySafeResponseConverter;
use Playwright\Dialog\DialogInterface;
use Playwright\Page\PageInterface;
use Playwright\Symfony\Client\BrowserRegistry;
use Playwright\Symfony\Client\BrowserSessionInterface;
use Playwright\Symfony\Client\Interception\AssetServer;
use Playwright\Symfony\Client\PlaywrightKernelClient;
use Playwright\Symfony\Client\RequestConverter;
use Symfony\Bundle\FrameworkBundle\Test\KernelTestCase;
use Zenstruck\Browser\PlaywrightBrowser;
use Zenstruck\Browser\Test\BrowserExtension;

use function file_put_contents;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function is_string;
use function json_encode;
use function sprintf;
use function sys_get_temp_dir;
use function unlink;

/**
 * Base for the tests that drive the WASM playground in a real browser.
 *
 * The playground is Stimulus-driven, so most steps reach the controllers through page scripts
 * rather than through the DOM. Playwright evaluates an expression, not a statement body, so every
 * script here is written as an arrow function.
 */
abstract class EndToEndTestCase extends KernelTestCase
{
    /** @var list<string> */
    private array $tempFiles = [];

    // the registry owns the node process and the browser, and creating one launches both. PHPUnit
    // builds a fresh test-case object per test, so holding it per instance launched a browser 165
    // times a run; the per-test isolation comes from the session, which is still closed each time.
    private static ?BrowserRegistry $registry = null;

    private ?BrowserSessionInterface $session = null;

    public static function tearDownAfterClass(): void
    {
        self::$registry?->close();
        self::$registry = null;

        parent::tearDownAfterClass();
    }

    protected function tearDown(): void
    {
        foreach ($this->tempFiles as $file) {
            @unlink($file);
        }

        $this->tempFiles = [];

        if (self::$registry !== null && $this->session !== null) {
            self::$registry->closeSession($this->session);
        }

        $this->session = null;

        parent::tearDown();
    }

    /**
     * Builds the browser directly instead of using zenstruck's HasBrowser trait.
     *
     * The trait hard-codes `new ResponseConverter()` (HasBrowser.php:220), so the container cannot
     * supply the fixed converter that BinarySafeResponseConverter provides - and without it every
     * binary asset reaches the browser base64-encoded and the playground never boots.
     */
    protected function playwrightBrowser(): PlaywrightBrowser
    {
        $kernel = self::bootKernel();
        $container = $kernel->getContainer();
        self::$registry ??= BrowserRegistry::fromEnvironment();
        $this->session ??= self::$registry->createSession();

        $hosts = type_list(type_string())->assert($container->getParameter('playwright.intercepted_hosts'));
        $baseUrl = $container->getParameter('playwright.base_url');
        $assets = $container->has(AssetServer::class)
            ? type_instance_of(AssetServer::class)->assert($container->get(AssetServer::class))
            : null;

        $browser = new PlaywrightBrowser(
            new PlaywrightKernelClient(
                $this->session,
                $kernel,
                new RequestConverter(),
                new BinarySafeResponseConverter(),
                [],
                $hosts,
                null,
                $assets,
                is_string($baseUrl) && $baseUrl !== '' ? $baseUrl : 'http://127.0.0.1',
            ),
        );

        BrowserExtension::registerBrowser($browser);

        return $browser;
    }

    protected function acceptDialogs(PageInterface $page): void
    {
        // Playwright dialogs are event-driven and auto-dismissed when unhandled, so the handler has
        // to be registered before the click that opens one - unlike Panther's post-hoc switchTo().
        $page->events()->onDialog(static fn(DialogInterface $dialog): mixed => $dialog->accept());
    }

    protected function clearLocalStorage(PageInterface $page): void
    {
        $page->evaluate(
            '() => window.Stimulus.getControllerForElementAndIdentifier(document.getElementById("playground"), "playground-storage").clearCode()',
        );
    }

    protected function createTempFile(string $filename, string $content): string
    {
        $path = sys_get_temp_dir() . '/' . $filename;
        file_put_contents($path, $content);
        $this->tempFiles[] = $path;

        return $path;
    }

    protected function getPlaygroundCode(PageInterface $page): string
    {
        return type_string()->assert($page->evaluate(
            '() => { const textarea = document.getElementById("code-editor");
             return window.Stimulus.getControllerForElementAndIdentifier(textarea, "code-editor").getCode(); }',
        ));
    }

    protected function openPlayground(string $path): PlaywrightBrowser
    {
        $browser = $this->playwrightBrowser()->visit($path);

        $this->waitForWasmReady($this->pageOf($browser));

        return $browser;
    }

    /**
     * Not zenstruck's attachFile(): that resolves the field through Mink's named-field finder
     * (id, name, label or value), and this input is hidden and addressed by a data attribute.
     *
     * @param list<string> $files
     */
    protected function upload(PageInterface $page, string $selector, array $files): void
    {
        $page->setInputFiles($selector, $files);
    }

    protected function pageOf(PlaywrightBrowser $browser): PageInterface
    {
        $page = $browser->client()->getPage();

        self::assertNotNull($page, 'the browser has no open page');

        return $page;
    }

    protected function setPlaygroundCode(PageInterface $page, string $code): void
    {
        $page->evaluate(sprintf('() => { const textarea = document.getElementById("code-editor");
             window.Stimulus.getControllerForElementAndIdentifier(textarea, "code-editor").setCode(%s);
             const playground = document.getElementById("playground");
             const storage = window.Stimulus.getControllerForElementAndIdentifier(playground, "playground-storage");
             if (storage) {
                 localStorage.setItem(storage.storageKeyValue || "flow-playground-code", %s);
             } }', json_encode($code), json_encode($code)));
    }

    protected function waitForWasmReady(PageInterface $page): void
    {
        $page->waitForFunction('() => { const playground = document.getElementById("playground");
             if (!playground || !window.Stimulus) { return false; }
             const wasm = window.Stimulus.getControllerForElementAndIdentifier(playground, "wasm");
             return Boolean(wasm) && wasm.isLoaded() && wasm.areResourcesLoaded(); }');
    }

    protected static function getKernelClass(): string
    {
        return Kernel::class;
    }
}
