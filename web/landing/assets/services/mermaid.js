let loadPromise = null;
let appliedTheme = null;

function currentTheme() {
    return document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'default';
}

function loadScript(src) {
    return new Promise((resolve, reject) => {
        const script = document.createElement('script');

        script.src = src;
        script.addEventListener('load', () => resolve(window.mermaid));
        script.addEventListener('error', () => reject(new Error(`Failed to load mermaid from "${src}"`)));

        document.head.appendChild(script);
    });
}

export async function mermaid(src) {
    loadPromise ??= loadScript(src);

    const instance = await loadPromise;
    const theme = currentTheme();

    if (theme !== appliedTheme) {
        instance.initialize({
            startOnLoad: false,
            theme,
            securityLevel: 'loose',
            flowchart: { useMaxWidth: true, htmlLabels: true },
        });

        appliedTheme = theme;
    }

    return instance;
}
