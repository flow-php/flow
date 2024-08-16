(function () {

    if (typeof Prism === 'undefined') {
        return;
    }

    const functions = window.flow.dsl.map(function (item) {
        return {
            name: item.name,
            slug: item.slug,
            module: item.attributes.find(attribute => attribute.name === 'DocumentationDSL')?.arguments.module.toLowerCase()
        };
    });

    // URL template
    const urlTemplate = "/documentation/dsl/{module_name}/{function_name}/#dsl-function";

    Prism.hooks.add('after-highlight', function(env) {

        if (env.language !== 'php') return;

        if (env.element.getAttribute('data-prismjs-no-auto-linker') !== null) {
            return;
        }

        // Find all tokens of type 'function' and 'function-method'
        const functionTokens = env.element.querySelectorAll('.token.function, .token.function-method');

        functionTokens.forEach(function(token) {
            const functionName = token.textContent;

            let definition = functions.find(f => f.name === functionName);

            if (definition) {
                const link = document.createElement('a');
                link.href = urlTemplate.replace('{module_name}', definition.module).replace('{function_name}', definition.slug).toLowerCase();
                link.target = '_blank';
                link.textContent = functionName;
                link.className = token.className;
                token.parentNode.replaceChild(link, token);
            }
        });
    });
}());