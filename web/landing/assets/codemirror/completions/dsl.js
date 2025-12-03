/**
 * CodeMirror Completer for Flow PHP DSL Functions
 *
 * Total functions: 372
 *
 * This completer provides autocompletion for all Flow PHP DSL functions:
 * - Extractors (flow-extractors)
 * - Loaders (flow-loaders)
 * - Transformers (flow-transformers)
 * - Scalar Functions (flow-scalar-functions)
 * - Aggregating Functions (flow-aggregating-functions)
 * - Window Functions (flow-window-functions)
 * - And more...
 */

import { CompletionContext, snippet } from "@codemirror/autocomplete"

// All DSL functions
const dslFunctions = [
        {
        label: "add_row_index",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtransformers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">add_row_index</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$column</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;index&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">StartFrom</span> <span class=\"fn-param\">$startFrom</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Transformation\\AddRowIndex\\StartFrom::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AddRowIndex</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\add_row_index(" + "$" + "{" + "1:column" + "}" + ", " + "$" + "{" + "2:startFrom" + "}" + ")"),
        boost: 10
    },        {
        label: "all",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">all</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$functions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">All</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\all(" + "$" + "{" + "1:functions" + "}" + ")"),
        boost: 10
    },        {
        label: "analyze",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">analyze</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Analyze</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\analyze()"),
        boost: 10
    },        {
        label: "any",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">any</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$values</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Any</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\any(" + "$" + "{" + "1:values" + "}" + ")"),
        boost: 10
    },        {
        label: "append",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">append</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for save_mode_append().
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\append()"),
        boost: 10
    },        {
        label: "array_exists",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_exists</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayPathExists</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed>|ScalarFunction $ref
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_exists(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:path" + "}" + ")"),
        boost: 10
    },        {
        label: "array_expand",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_expand</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ArrayExpand</span> <span class=\"fn-param\">$expand</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Function\\ArrayExpand\\ArrayExpand::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayExpand</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Expands each value into entry, if there are more than one value, multiple rows will be created.<br>Array keys are ignored, only values are used to create new rows.<br>Before:<br>  +--+-------------------+<br>  |id|              array|<br>  +--+-------------------+<br>  | 1|{\"a\":1,\"b\":2,\"c\":3}|<br>  +--+-------------------+<br>After:<br>  +--+--------+<br>  |id|expanded|<br>  +--+--------+<br>  | 1|       1|<br>  | 1|       2|<br>  | 1|       3|<br>  +--+--------+
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_expand(" + "$" + "{" + "1:function" + "}" + ", " + "$" + "{" + "2:expand" + "}" + ")"),
        boost: 10
    },        {
        label: "array_get",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_get</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayGet</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_get(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:path" + "}" + ")"),
        boost: 10
    },        {
        label: "array_get_collection",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_get_collection</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$keys</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayGetCollection</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed>|ScalarFunction $keys
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_get_collection(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:keys" + "}" + ")"),
        boost: 10
    },        {
        label: "array_get_collection_first",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_get_collection_first</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$keys</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayGetCollection</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_get_collection_first(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:keys" + "}" + ")"),
        boost: 10
    },        {
        label: "array_keys_style_convert",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_keys_style_convert</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">StringStyles|StringStyles|string</span> <span class=\"fn-param\">$style</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\String\\StringStyles::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayKeysStyleConvert</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_keys_style_convert(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:style" + "}" + ")"),
        boost: 10
    },        {
        label: "array_key_rename",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_key_rename</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$newName</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayKeyRename</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_key_rename(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:path" + "}" + ", " + "$" + "{" + "3:newName" + "}" + ")"),
        boost: 10
    },        {
        label: "array_merge",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_merge</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayMerge</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed>|ScalarFunction $left<br>@param array<array-key, mixed>|ScalarFunction $right
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_merge(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },        {
        label: "array_merge_collection",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_merge_collection</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$array</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayMergeCollection</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed>|ScalarFunction $array
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_merge_collection(" + "$" + "{" + "1:array" + "}" + ")"),
        boost: 10
    },        {
        label: "array_reverse",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_reverse</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$preserveKeys</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayReverse</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed>|ScalarFunction $function
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_reverse(" + "$" + "{" + "1:function" + "}" + ", " + "$" + "{" + "2:preserveKeys" + "}" + ")"),
        boost: 10
    },        {
        label: "array_sort",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_sort</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|Sort|null</span> <span class=\"fn-param\">$sort_function</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int|null</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$recursive</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArraySort</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_sort(" + "$" + "{" + "1:function" + "}" + ", " + "$" + "{" + "2:sort_function" + "}" + ", " + "$" + "{" + "3:flags" + "}" + ", " + "$" + "{" + "4:recursive" + "}" + ")"),
        boost: 10
    },        {
        label: "array_to_generator",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_to_generator</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Generator</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param array<T> $data<br>@return \\Generator<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Parquet\\array_to_generator(" + "$" + "{" + "1:data" + "}" + ")"),
        boost: 10
    },        {
        label: "array_to_row",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_to_row</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">EntryFactory</span> <span class=\"fn-param\">$entryFactory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Partitions|array</span> <span class=\"fn-param\">$partitions</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Row</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array<mixed>>|array<mixed|string> $data<br>@param array<Partition>|Partitions $partitions<br>@param null|Schema $schema
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_to_row(" + "$" + "{" + "1:data" + "}" + ", " + "$" + "{" + "2:entryFactory" + "}" + ", " + "$" + "{" + "3:partitions" + "}" + ", " + "$" + "{" + "4:schema" + "}" + ")"),
        boost: 10
    },        {
        label: "array_to_rows",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_to_rows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">EntryFactory</span> <span class=\"fn-param\">$entryFactory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Partitions|array</span> <span class=\"fn-param\">$partitions</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Rows</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array<mixed>>|array<mixed|string> $data<br>@param array<Partition>|Partitions $partitions<br>@param null|Schema $schema
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_to_rows(" + "$" + "{" + "1:data" + "}" + ", " + "$" + "{" + "2:entryFactory" + "}" + ", " + "$" + "{" + "3:partitions" + "}" + ", " + "$" + "{" + "4:schema" + "}" + ")"),
        boost: 10
    },        {
        label: "array_unpack",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_unpack</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$array</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$skip_keys</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string|null</span> <span class=\"fn-param\">$entry_prefix</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayUnpack</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed>|ScalarFunction $array<br>@param array<array-key, mixed>|ScalarFunction $skip_keys
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_unpack(" + "$" + "{" + "1:array" + "}" + ", " + "$" + "{" + "2:skip_keys" + "}" + ", " + "$" + "{" + "3:entry_prefix" + "}" + ")"),
        boost: 10
    },        {
        label: "average",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">average</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$scale</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">2</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Rounding</span> <span class=\"fn-param\">$rounding</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Calculator\\Rounding::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Average</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\average(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:scale" + "}" + ", " + "$" + "{" + "3:rounding" + "}" + ")"),
        boost: 10
    },        {
        label: "aws_s3_client",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">aws_s3_client</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$configuration</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">S3Client</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string, mixed> $configuration - for details please see https://async-aws.com/clients/s3.html
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\Bridge\\AsyncAWS\\DSL\\aws_s3_client(" + "$" + "{" + "1:configuration" + "}" + ")"),
        boost: 10
    },        {
        label: "aws_s3_filesystem",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">aws_s3_filesystem</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$bucket</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">S3Client</span> <span class=\"fn-param\">$s3Client</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Filesystem\\Bridge\\AsyncAWS\\Options::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AsyncAWSS3Filesystem</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\Bridge\\AsyncAWS\\DSL\\aws_s3_filesystem(" + "$" + "{" + "1:bucket" + "}" + ", " + "$" + "{" + "2:s3Client" + "}" + ", " + "$" + "{" + "3:options" + "}" + ")"),
        boost: 10
    },        {
        label: "azure_blob_service",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">azure_blob_service</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Configuration</span> <span class=\"fn-param\">$configuration</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">AuthorizationFactory</span> <span class=\"fn-param\">$azure_authorization_factory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ClientInterface</span> <span class=\"fn-param\">$client</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">HttpFactory</span> <span class=\"fn-param\">$azure_http_factory</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">URLFactory</span> <span class=\"fn-param\">$azure_url_factory</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">LoggerInterface</span> <span class=\"fn-param\">$logger</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BlobServiceInterface</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Azure\\SDK\\DSL\\azure_blob_service(" + "$" + "{" + "1:configuration" + "}" + ", " + "$" + "{" + "2:azure_authorization_factory" + "}" + ", " + "$" + "{" + "3:client" + "}" + ", " + "$" + "{" + "4:azure_http_factory" + "}" + ", " + "$" + "{" + "5:azure_url_factory" + "}" + ", " + "$" + "{" + "6:logger" + "}" + ")"),
        boost: 10
    },        {
        label: "azure_blob_service_config",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">azure_blob_service_config</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$account</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$container</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Configuration</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Azure\\SDK\\DSL\\azure_blob_service_config(" + "$" + "{" + "1:account" + "}" + ", " + "$" + "{" + "2:container" + "}" + ")"),
        boost: 10
    },        {
        label: "azure_filesystem",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">azure_filesystem</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">BlobServiceInterface</span> <span class=\"fn-param\">$blob_service</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Filesystem\\Bridge\\Azure\\Options::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AzureBlobFilesystem</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\Bridge\\Azure\\DSL\\azure_filesystem(" + "$" + "{" + "1:blob_service" + "}" + ", " + "$" + "{" + "2:options" + "}" + ")"),
        boost: 10
    },        {
        label: "azure_filesystem_options",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">azure_filesystem_options</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Options</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\Bridge\\Azure\\DSL\\azure_filesystem_options()"),
        boost: 10
    },        {
        label: "azure_http_factory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">azure_http_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">RequestFactoryInterface</span> <span class=\"fn-param\">$request_factory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">StreamFactoryInterface</span> <span class=\"fn-param\">$stream_factory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">HttpFactory</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Azure\\SDK\\DSL\\azure_http_factory(" + "$" + "{" + "1:request_factory" + "}" + ", " + "$" + "{" + "2:stream_factory" + "}" + ")"),
        boost: 10
    },        {
        label: "azure_shared_key_authorization_factory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">azure_shared_key_authorization_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$account</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$key</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SharedKeyFactory</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Azure\\SDK\\DSL\\azure_shared_key_authorization_factory(" + "$" + "{" + "1:account" + "}" + ", " + "$" + "{" + "2:key" + "}" + ")"),
        boost: 10
    },        {
        label: "azure_url_factory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">azure_url_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$host</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;blob.core.windows.net&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AzureURLFactory</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Azure\\SDK\\DSL\\azure_url_factory(" + "$" + "{" + "1:host" + "}" + ")"),
        boost: 10
    },        {
        label: "azurite_url_factory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">azurite_url_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$host</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;localhost&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$port</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;10000&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$secure</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AzuriteURLFactory</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Azure\\SDK\\DSL\\azurite_url_factory(" + "$" + "{" + "1:host" + "}" + ", " + "$" + "{" + "2:port" + "}" + ", " + "$" + "{" + "3:secure" + "}" + ")"),
        boost: 10
    },        {
        label: "bar_chart",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">bar_chart</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference</span> <span class=\"fn-param\">$label</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">References</span> <span class=\"fn-param\">$datasets</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BarChart</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\ChartJS\\bar_chart(" + "$" + "{" + "1:label" + "}" + ", " + "$" + "{" + "2:datasets" + "}" + ")"),
        boost: 10
    },        {
        label: "batched_by",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">batched_by</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$extractor</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$min_size</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BatchByExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param null|int<1, max> $min_size
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\batched_by(" + "$" + "{" + "1:extractor" + "}" + ", " + "$" + "{" + "2:column" + "}" + ", " + "$" + "{" + "3:min_size" + "}" + ")"),
        boost: 10
    },        {
        label: "batches",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">batches</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$extractor</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$size</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BatchExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param int<1, max> $size
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\batches(" + "$" + "{" + "1:extractor" + "}" + ", " + "$" + "{" + "2:size" + "}" + ")"),
        boost: 10
    },        {
        label: "batch_size",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtransformers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">batch_size</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$size</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BatchSize</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param int<1, max> $size
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\batch_size(" + "$" + "{" + "1:size" + "}" + ")"),
        boost: 10
    },        {
        label: "between",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">between</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$lower_bound</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$upper_bound</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|Boundary</span> <span class=\"fn-param\">$boundary</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Function\\Between\\Boundary::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Between</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\between(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:lower_bound" + "}" + ", " + "$" + "{" + "3:upper_bound" + "}" + ", " + "$" + "{" + "4:boundary" + "}" + ")"),
        boost: 10
    },        {
        label: "boolean_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">boolean_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?bool>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\boolean_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "bool_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">bool_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?bool>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\bool_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "bool_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">bool_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Definition<bool>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\bool_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "call",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">call</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|callable</span> <span class=\"fn-param\">$callable</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$return_type</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CallUserFunc</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Calls a user-defined function with the given parameters.<br>@param callable|ScalarFunction $callable<br>@param array<mixed> $parameters<br>@param null|Type<mixed> $return_type
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\call(" + "$" + "{" + "1:callable" + "}" + ", " + "$" + "{" + "2:parameters" + "}" + ", " + "$" + "{" + "3:return_type" + "}" + ")"),
        boost: 10
    },        {
        label: "capitalize",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">capitalize</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Capitalize</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\capitalize(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "cast",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">cast</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type|string</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Cast</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param \\Flow\\Types\\Type<mixed>|string $type
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\cast(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:type" + "}" + ")"),
        boost: 10
    },        {
        label: "chunks_from",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">chunks_from</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$extractor</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$chunk_size</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BatchExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param int<1, max> $chunk_size<br>@deprecated use batches() instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\chunks_from(" + "$" + "{" + "1:extractor" + "}" + ", " + "$" + "{" + "2:chunk_size" + "}" + ")"),
        boost: 10
    },        {
        label: "coalesce",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">coalesce</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$values</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Coalesce</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\coalesce(" + "$" + "{" + "1:values" + "}" + ")"),
        boost: 10
    },        {
        label: "col",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">col</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EntryReference</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    An alias for \`ref\`.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\col(" + "$" + "{" + "1:entry" + "}" + ")"),
        boost: 10
    },        {
        label: "collect",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">collect</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Collect</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\collect(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "collect_unique",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">collect_unique</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CollectUnique</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\collect_unique(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "combine",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">combine</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$keys</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$values</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Combine</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed>|ScalarFunction $keys<br>@param array<array-key, mixed>|ScalarFunction $values
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\combine(" + "$" + "{" + "1:keys" + "}" + ", " + "$" + "{" + "2:values" + "}" + ")"),
        boost: 10
    },        {
        label: "compare_all",
        type: "function",
        detail: "flow\u002Ddsl\u002Dcomparisons",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">compare_all</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Comparison</span> <span class=\"fn-param\">$comparisons</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">All</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\compare_all(" + "$" + "{" + "1:comparisons" + "}" + ")"),
        boost: 10
    },        {
        label: "compare_any",
        type: "function",
        detail: "flow\u002Ddsl\u002Dcomparisons",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">compare_any</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Comparison</span> <span class=\"fn-param\">$comparisons</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Any</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\compare_any(" + "$" + "{" + "1:comparisons" + "}" + ")"),
        boost: 10
    },        {
        label: "compare_entries_by_name",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">compare_entries_by_name</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Order</span> <span class=\"fn-param\">$order</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Transformer\\OrderEntries\\Order::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparator</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\compare_entries_by_name(" + "$" + "{" + "1:order" + "}" + ")"),
        boost: 10
    },        {
        label: "compare_entries_by_name_desc",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">compare_entries_by_name_desc</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparator</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\compare_entries_by_name_desc()"),
        boost: 10
    },        {
        label: "compare_entries_by_type",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">compare_entries_by_type</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$priorities</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[...]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Order</span> <span class=\"fn-param\">$order</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Transformer\\OrderEntries\\Order::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparator</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<class-string<Entry<mixed>>, int> $priorities
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\compare_entries_by_type(" + "$" + "{" + "1:priorities" + "}" + ", " + "$" + "{" + "2:order" + "}" + ")"),
        boost: 10
    },        {
        label: "compare_entries_by_type_and_name",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">compare_entries_by_type_and_name</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$priorities</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[...]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Order</span> <span class=\"fn-param\">$order</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Transformer\\OrderEntries\\Order::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparator</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<class-string<Entry<mixed>>, int> $priorities
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\compare_entries_by_type_and_name(" + "$" + "{" + "1:priorities" + "}" + ", " + "$" + "{" + "2:order" + "}" + ")"),
        boost: 10
    },        {
        label: "compare_entries_by_type_desc",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">compare_entries_by_type_desc</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$priorities</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[...]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparator</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<class-string<Entry<mixed>>, int> $priorities
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\compare_entries_by_type_desc(" + "$" + "{" + "1:priorities" + "}" + ")"),
        boost: 10
    },        {
        label: "concat",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">concat</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$functions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Concat</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Concat all values. If you want to concatenate values with separator use concat_ws function.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\concat(" + "$" + "{" + "1:functions" + "}" + ")"),
        boost: 10
    },        {
        label: "concat_ws",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">concat_ws</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$separator</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$functions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConcatWithSeparator</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Concat all values with separator.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\concat_ws(" + "$" + "{" + "1:separator" + "}" + ", " + "$" + "{" + "2:functions" + "}" + ")"),
        boost: 10
    },        {
        label: "config",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">config</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Config</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\config()"),
        boost: 10
    },        {
        label: "config_builder",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">config_builder</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConfigBuilder</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\config_builder()"),
        boost: 10
    },        {
        label: "constraint_sorted_by",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">constraint_sorted_by</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$columns</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SortedByConstraint</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\constraint_sorted_by(" + "$" + "{" + "1:column" + "}" + ", " + "$" + "{" + "2:columns" + "}" + ")"),
        boost: 10
    },        {
        label: "constraint_unique",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">constraint_unique</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$reference</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$references</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">UniqueConstraint</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\constraint_unique(" + "$" + "{" + "1:reference" + "}" + ", " + "$" + "{" + "2:references" + "}" + ")"),
        boost: 10
    },        {
        label: "count",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">count</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference</span> <span class=\"fn-param\">$function</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Count</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\count(" + "$" + "{" + "1:function" + "}" + ")"),
        boost: 10
    },        {
        label: "csv_detect_separator",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">csv_detect_separator</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SourceStream</span> <span class=\"fn-param\">$stream</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$lines</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">5</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Option</span> <span class=\"fn-param\">$fallback</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Adapter\\CSV\\Detector\\Option::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Option</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param SourceStream $stream - valid resource to CSV file<br>@param int<1, max> $lines - number of lines to read from CSV file, default 5, more lines means more accurate detection but slower detection<br>@param null|Option $fallback - fallback option to use when no best option can be detected, default is Option(\',\', \'\"\', \'\\\\\')<br>@param null|Options $options - options to use for detection, default is Options::all()
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\CSV\\csv_detect_separator(" + "$" + "{" + "1:stream" + "}" + ", " + "$" + "{" + "2:lines" + "}" + ", " + "$" + "{" + "3:fallback" + "}" + ", " + "$" + "{" + "4:options" + "}" + ")"),
        boost: 10
    },        {
        label: "data_frame",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_frame</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Config|ConfigBuilder|null</span> <span class=\"fn-param\">$config</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Flow</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\data_frame(" + "$" + "{" + "1:config" + "}" + ")"),
        boost: 10
    },        {
        label: "datetime_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">datetime_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateTimeInterface|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?\\DateTimeInterface>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\datetime_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "datetime_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">datetime_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Definition<\\DateTimeInterface>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\datetime_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "date_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">date_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateTimeInterface|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?\\DateTimeInterface>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\date_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "date_interval_to_microseconds",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">date_interval_to_microseconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DateInterval</span> <span class=\"fn-param\">$interval</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">int</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\date_interval_to_microseconds(" + "$" + "{" + "1:interval" + "}" + ")"),
        boost: 10
    },        {
        label: "date_interval_to_milliseconds",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">date_interval_to_milliseconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DateInterval</span> <span class=\"fn-param\">$interval</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">int</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\date_interval_to_milliseconds(" + "$" + "{" + "1:interval" + "}" + ")"),
        boost: 10
    },        {
        label: "date_interval_to_seconds",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">date_interval_to_seconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DateInterval</span> <span class=\"fn-param\">$interval</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">int</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\date_interval_to_seconds(" + "$" + "{" + "1:interval" + "}" + ")"),
        boost: 10
    },        {
        label: "date_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">date_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Definition<\\DateTimeInterface>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\date_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "date_time_format",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">date_time_format</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$format</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DateTimeFormat</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\date_time_format(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:format" + "}" + ")"),
        boost: 10
    },        {
        label: "dbal_dataframe_factory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dbal_dataframe_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection|array</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">QueryParameter</span> <span class=\"fn-param\">$parameters</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalDataFrameFactory</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string, mixed>|Connection $connection<br>@param string $query<br>@param QueryParameter ...$parameters
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\dbal_dataframe_factory(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:query" + "}" + ", " + "$" + "{" + "3:parameters" + "}" + ")"),
        boost: 10
    },        {
        label: "dbal_from_queries",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dbal_from_queries</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ParametersSet</span> <span class=\"fn-param\">$parameters_set</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalQueryExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @deprecated use from_dbal_queries() instead<br>@param null|ParametersSet $parameters_set - each one parameters array will be evaluated as new query<br>@param array<int|string, DbalArrayType|DbalParameterType|DbalType|int|string> $types
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\dbal_from_queries(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:query" + "}" + ", " + "$" + "{" + "3:parameters_set" + "}" + ", " + "$" + "{" + "4:types" + "}" + ")"),
        boost: 10
    },        {
        label: "dbal_from_query",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dbal_from_query</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalQueryExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @deprecated use from_dbal_query() instead<br>@param array<string, mixed>|list<mixed> $parameters - @deprecated use DbalQueryExtractor::withParameters() instead<br>@param array<int<0, max>|string, DbalArrayType|DbalParameterType|DbalType|string> $types - @deprecated use DbalQueryExtractor::withTypes() instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\dbal_from_query(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:query" + "}" + ", " + "$" + "{" + "3:parameters" + "}" + ", " + "$" + "{" + "4:types" + "}" + ")"),
        boost: 10
    },        {
        label: "delay_exponential",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">delay_exponential</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Duration</span> <span class=\"fn-param\">$base</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$multiplier</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">2</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Duration</span> <span class=\"fn-param\">$max_delay</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Exponential</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\delay_exponential(" + "$" + "{" + "1:base" + "}" + ", " + "$" + "{" + "2:multiplier" + "}" + ", " + "$" + "{" + "3:max_delay" + "}" + ")"),
        boost: 10
    },        {
        label: "delay_fixed",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">delay_fixed</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Duration</span> <span class=\"fn-param\">$delay</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Fixed</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\delay_fixed(" + "$" + "{" + "1:delay" + "}" + ")"),
        boost: 10
    },        {
        label: "delay_jitter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">delay_jitter</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DelayFactory</span> <span class=\"fn-param\">$delay</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">float</span> <span class=\"fn-param\">$jitter_factor</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Jitter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param float $jitter_factor a value between 0 and 1 representing the maximum percentage of jitter to apply
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\delay_jitter(" + "$" + "{" + "1:delay" + "}" + ", " + "$" + "{" + "2:jitter_factor" + "}" + ")"),
        boost: 10
    },        {
        label: "delay_linear",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">delay_linear</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Duration</span> <span class=\"fn-param\">$delay</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Duration</span> <span class=\"fn-param\">$increment</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Linear</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\delay_linear(" + "$" + "{" + "1:delay" + "}" + ", " + "$" + "{" + "2:increment" + "}" + ")"),
        boost: 10
    },        {
        label: "dense_rank",
        type: "function",
        detail: "flow\u002Ddsl\u002Dwindow\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dense_rank</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DenseRank</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\dense_rank()"),
        boost: 10
    },        {
        label: "dens_rank",
        type: "function",
        detail: "flow\u002Ddsl\u002Dwindow\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dens_rank</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DenseRank</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\dens_rank()"),
        boost: 10
    },        {
        label: "df",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">df</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Config|ConfigBuilder|null</span> <span class=\"fn-param\">$config</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Flow</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for data_frame() : Flow.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\df(" + "$" + "{" + "1:config" + "}" + ")"),
        boost: 10
    },        {
        label: "dom_element_to_string",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dom_element_to_string</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DOMElement</span> <span class=\"fn-param\">$element</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$format_output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$preserver_white_space</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string|false</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @deprecated Please use \\Flow\\Types\\DSL\\dom_element_to_string() instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\dom_element_to_string(" + "$" + "{" + "1:element" + "}" + ", " + "$" + "{" + "2:format_output" + "}" + ", " + "$" + "{" + "3:preserver_white_space" + "}" + ")"),
        boost: 10
    },        {
        label: "dom_element_to_string",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dom_element_to_string</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DOMElement</span> <span class=\"fn-param\">$element</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$format_output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$preserver_white_space</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string|false</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\dom_element_to_string(" + "$" + "{" + "1:element" + "}" + ", " + "$" + "{" + "2:format_output" + "}" + ", " + "$" + "{" + "3:preserver_white_space" + "}" + ")"),
        boost: 10
    },        {
        label: "drop",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtransformers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">drop</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Drop</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\drop(" + "$" + "{" + "1:entries" + "}" + ")"),
        boost: 10
    },        {
        label: "duration_microseconds",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">duration_microseconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$microseconds</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Duration</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\duration_microseconds(" + "$" + "{" + "1:microseconds" + "}" + ")"),
        boost: 10
    },        {
        label: "duration_milliseconds",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">duration_milliseconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$milliseconds</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Duration</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\duration_milliseconds(" + "$" + "{" + "1:milliseconds" + "}" + ")"),
        boost: 10
    },        {
        label: "duration_minutes",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">duration_minutes</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$minutes</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Duration</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\duration_minutes(" + "$" + "{" + "1:minutes" + "}" + ")"),
        boost: 10
    },        {
        label: "duration_seconds",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">duration_seconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$seconds</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Duration</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\duration_seconds(" + "$" + "{" + "1:seconds" + "}" + ")"),
        boost: 10
    },        {
        label: "empty_generator",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">empty_generator</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Generator</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Parquet\\empty_generator()"),
        boost: 10
    },        {
        label: "entries",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">entries</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Entry</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entries</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Entry<mixed> ...$entries
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\entries(" + "$" + "{" + "1:entries" + "}" + ")"),
        boost: 10
    },        {
        label: "entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EntryReference</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    An alias for \`ref\`.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\entry(" + "$" + "{" + "1:entry" + "}" + ")"),
        boost: 10
    },        {
        label: "entry_id_factory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">entry_id_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry_name</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IdFactory</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Elasticsearch\\entry_id_factory(" + "$" + "{" + "1:entry_name" + "}" + ")"),
        boost: 10
    },        {
        label: "enum_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">enum_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">UnitEnum</span> <span class=\"fn-param\">$enum</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?\\UnitEnum>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\enum_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:enum" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "enum_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">enum_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T of \\UnitEnum<br>@param class-string<T> $type<br>@return Definition<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\enum_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:type" + "}" + ", " + "$" + "{" + "3:nullable" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "equal",
        type: "function",
        detail: "flow\u002Ddsl\u002Dcomparisons",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">equal</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Equal</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\equal(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },        {
        label: "es_hits_to_rows",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">es_hits_to_rows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DocumentDataSource</span> <span class=\"fn-param\">$source</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Adapter\\Elasticsearch\\ElasticsearchPHP\\DocumentDataSource::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">HitsIntoRowsTransformer</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Transforms elasticsearch results into clear Flow Rows using [\'hits\'][\'hits\'][x][\'_source\'].<br>@return HitsIntoRowsTransformer
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Elasticsearch\\es_hits_to_rows(" + "$" + "{" + "1:source" + "}" + ")"),
        boost: 10
    },        {
        label: "exception_if_exists",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">exception_if_exists</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for save_mode_exception_if_exists().
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\exception_if_exists()"),
        boost: 10
    },        {
        label: "execution_context",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">execution_context</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Config</span> <span class=\"fn-param\">$config</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FlowContext</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\execution_context(" + "$" + "{" + "1:config" + "}" + ")"),
        boost: 10
    },        {
        label: "execution_lenient",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">execution_lenient</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ExecutionMode</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    In this mode, functions returns nulls instead of throwing exceptions.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\execution_lenient()"),
        boost: 10
    },        {
        label: "execution_strict",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">execution_strict</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ExecutionMode</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    In this mode, functions throws exceptions if the given entry is not found<br>or passed parameters are invalid.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\execution_strict()"),
        boost: 10
    },        {
        label: "exists",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">exists</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Exists</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\exists(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "files",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">files</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$directory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FilesExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\files(" + "$" + "{" + "1:directory" + "}" + ")"),
        boost: 10
    },        {
        label: "filesystem_cache",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">filesystem_cache</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string|null</span> <span class=\"fn-param\">$cache_dir</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Filesystem</span> <span class=\"fn-param\">$filesystem</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Filesystem\\Local\\NativeLocalFilesystem::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Serializer</span> <span class=\"fn-param\">$serializer</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Serializer\\NativePHPSerializer::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FilesystemCache</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\filesystem_cache(" + "$" + "{" + "1:cache_dir" + "}" + ", " + "$" + "{" + "2:filesystem" + "}" + ", " + "$" + "{" + "3:serializer" + "}" + ")"),
        boost: 10
    },        {
        label: "first",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">first</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">First</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\first(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "float_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">float_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string|int|float|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?float>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\float_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "float_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">float_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Definition<float>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\float_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "flow_context",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">flow_context</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Config</span> <span class=\"fn-param\">$config</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FlowContext</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\flow_context(" + "$" + "{" + "1:config" + "}" + ")"),
        boost: 10
    },        {
        label: "from_all",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_all</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$extractors</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ChainExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_all(" + "$" + "{" + "1:extractors" + "}" + ")"),
        boost: 10
    },        {
        label: "from_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_array</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">iterable</span> <span class=\"fn-param\">$array</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param iterable<array<mixed>> $array<br>@param null|Schema $schema - @deprecated use withSchema() method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_array(" + "$" + "{" + "1:array" + "}" + ", " + "$" + "{" + "2:schema" + "}" + ")"),
        boost: 10
    },        {
        label: "from_avro",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_avro</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AvroExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\Adapter\\Avro\\from_avro(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },        {
        label: "from_cache",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_cache</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$id</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$fallback_extractor</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$clear</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CacheExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param string $id - cache id from which data will be extracted<br>@param null|Extractor $fallback_extractor - extractor that will be used when cache is empty - @deprecated use withFallbackExtractor() method instead<br>@param bool $clear - clear cache after extraction - @deprecated use withClearOnFinish() method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_cache(" + "$" + "{" + "1:id" + "}" + ", " + "$" + "{" + "2:fallback_extractor" + "}" + ", " + "$" + "{" + "3:clear" + "}" + ")"),
        boost: 10
    },        {
        label: "from_csv",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_csv</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$with_header</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$empty_to_null</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$enclosure</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$escape</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$characters_read_in_line</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1000</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CSVExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $path<br>@param bool $empty_to_null - @deprecated use $loader->withEmptyToNull() instead<br>@param bool $with_header - @deprecated use $loader->withHeader() instead<br>@param null|string $separator - @deprecated use $loader->withSeparator() instead<br>@param null|string $enclosure - @deprecated use $loader->withEnclosure() instead<br>@param null|string $escape - @deprecated use $loader->withEscape() instead<br>@param int<1, max> $characters_read_in_line - @deprecated use $loader->withCharactersReadInLine() instead<br>@param null|Schema $schema - @deprecated use $loader->withSchema() instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\CSV\\from_csv(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:with_header" + "}" + ", " + "$" + "{" + "3:empty_to_null" + "}" + ", " + "$" + "{" + "4:separator" + "}" + ", " + "$" + "{" + "5:enclosure" + "}" + ", " + "$" + "{" + "6:escape" + "}" + ", " + "$" + "{" + "7:characters_read_in_line" + "}" + ", " + "$" + "{" + "8:schema" + "}" + ")"),
        boost: 10
    },        {
        label: "from_data_frame",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_data_frame</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DataFrame</span> <span class=\"fn-param\">$data_frame</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataFrameExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_data_frame(" + "$" + "{" + "1:data_frame" + "}" + ")"),
        boost: 10
    },        {
        label: "from_dbal_key_set_qb",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_dbal_key_set_qb</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">QueryBuilder</span> <span class=\"fn-param\">$queryBuilder</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">KeySet</span> <span class=\"fn-param\">$key_set</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalKeySetExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\from_dbal_key_set_qb(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:queryBuilder" + "}" + ", " + "$" + "{" + "3:key_set" + "}" + ")"),
        boost: 10
    },        {
        label: "from_dbal_limit_offset",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_dbal_limit_offset</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Table|string</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">OrderBy|array</span> <span class=\"fn-param\">$order_by</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$page_size</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1000</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$maximum</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalLimitOffsetExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Connection $connection<br>@param string|Table $table<br>@param array<OrderBy>|OrderBy $order_by<br>@param int $page_size<br>@param null|int $maximum<br>@throws InvalidArgumentException
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\from_dbal_limit_offset(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:table" + "}" + ", " + "$" + "{" + "3:order_by" + "}" + ", " + "$" + "{" + "4:page_size" + "}" + ", " + "$" + "{" + "5:maximum" + "}" + ")"),
        boost: 10
    },        {
        label: "from_dbal_limit_offset_qb",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_dbal_limit_offset_qb</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">QueryBuilder</span> <span class=\"fn-param\">$queryBuilder</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$page_size</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1000</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$maximum</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalLimitOffsetExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Connection $connection<br>@param int $page_size<br>@param null|int $maximum - maximum can also be taken from a query builder, $maximum however is used regardless of the query builder if it\'s set<br>@param int $offset - offset can also be taken from a query builder, $offset however is used regardless of the query builder if it\'s set to non 0 value
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\from_dbal_limit_offset_qb(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:queryBuilder" + "}" + ", " + "$" + "{" + "3:page_size" + "}" + ", " + "$" + "{" + "4:maximum" + "}" + ", " + "$" + "{" + "5:offset" + "}" + ")"),
        boost: 10
    },        {
        label: "from_dbal_queries",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_dbal_queries</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ParametersSet</span> <span class=\"fn-param\">$parameters_set</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalQueryExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param null|ParametersSet $parameters_set - each one parameters array will be evaluated as new query<br>@param array<int|string, DbalArrayType|DbalParameterType|DbalType|int|string> $types
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\from_dbal_queries(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:query" + "}" + ", " + "$" + "{" + "3:parameters_set" + "}" + ", " + "$" + "{" + "4:types" + "}" + ")"),
        boost: 10
    },        {
        label: "from_dbal_query",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_dbal_query</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalQueryExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string, mixed>|list<mixed> $parameters - @deprecated use DbalQueryExtractor::withParameters() instead<br>@param array<int<0, max>|string, DbalArrayType|DbalParameterType|DbalType|string> $types - @deprecated use DbalQueryExtractor::withTypes() instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\from_dbal_query(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:query" + "}" + ", " + "$" + "{" + "3:parameters" + "}" + ", " + "$" + "{" + "4:types" + "}" + ")"),
        boost: 10
    },        {
        label: "from_dynamic_http_requests",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_dynamic_http_requests</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ClientInterface</span> <span class=\"fn-param\">$client</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">NextRequestFactory</span> <span class=\"fn-param\">$requestFactory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PsrHttpClientDynamicExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Http\\from_dynamic_http_requests(" + "$" + "{" + "1:client" + "}" + ", " + "$" + "{" + "2:requestFactory" + "}" + ")"),
        boost: 10
    },        {
        label: "from_es",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_es</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$pit_params</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ElasticsearchExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Extractor will automatically try to iterate over whole index using one of the two iteration methods:.<br>- from/size<br>- search_after<br>Search after is selected when you provide define sort parameters in query, otherwise it will fallback to from/size.<br>@param array{<br> hosts?: array<string>,<br> connectionParams?: array<mixed>,<br> retries?: int,<br> sniffOnStart?: bool,<br> sslCert?: array<string>,<br> sslKey?: array<string>,<br> sslVerification?: bool|string,<br> elasticMetaHeader?: bool,<br> includePortInHostHeader?: bool<br>} $config<br>@param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/search-search.html<br>@param ?array<mixed> $pit_params - when used extractor will create point in time to stabilize search results. Point in time is automatically closed when last element is extracted. https://www.elastic.co/guide/en/elasticsearch/reference/master/point-in-time-api.html - @deprecated use withPointInTime method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Elasticsearch\\from_es(" + "$" + "{" + "1:config" + "}" + ", " + "$" + "{" + "2:parameters" + "}" + ", " + "$" + "{" + "3:pit_params" + "}" + ")"),
        boost: 10
    },        {
        label: "from_excel",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_excel</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ExcelExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Excel\\DSL\\from_excel(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },        {
        label: "from_google_sheet",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_google_sheet</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Sheets|array</span> <span class=\"fn-param\">$auth_config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$spreadsheet_id</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$sheet_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$with_header</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$rows_per_page</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1000</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">GoogleSheetExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string}|Sheets $auth_config<br>@param string $spreadsheet_id<br>@param string $sheet_name<br>@param bool $with_header - @deprecated use withHeader method instead<br>@param int $rows_per_page - how many rows per page to fetch from Google Sheets API - @deprecated use withRowsPerPage method instead<br>@param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options - @deprecated use withOptions method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\GoogleSheet\\from_google_sheet(" + "$" + "{" + "1:auth_config" + "}" + ", " + "$" + "{" + "2:spreadsheet_id" + "}" + ", " + "$" + "{" + "3:sheet_name" + "}" + ", " + "$" + "{" + "4:with_header" + "}" + ", " + "$" + "{" + "5:rows_per_page" + "}" + ", " + "$" + "{" + "6:options" + "}" + ")"),
        boost: 10
    },        {
        label: "from_google_sheet_columns",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_google_sheet_columns</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Sheets|array</span> <span class=\"fn-param\">$auth_config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$spreadsheet_id</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$sheet_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$start_range_column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$end_range_column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$with_header</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$rows_per_page</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1000</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">GoogleSheetExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string}|Sheets $auth_config<br>@param string $spreadsheet_id<br>@param string $sheet_name<br>@param string $start_range_column<br>@param string $end_range_column<br>@param bool $with_header - @deprecated use withHeader method instead<br>@param int $rows_per_page - how many rows per page to fetch from Google Sheets API, default 1000 - @deprecated use withRowsPerPage method instead<br>@param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options - @deprecated use withOptions method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\GoogleSheet\\from_google_sheet_columns(" + "$" + "{" + "1:auth_config" + "}" + ", " + "$" + "{" + "2:spreadsheet_id" + "}" + ", " + "$" + "{" + "3:sheet_name" + "}" + ", " + "$" + "{" + "4:start_range_column" + "}" + ", " + "$" + "{" + "5:end_range_column" + "}" + ", " + "$" + "{" + "6:with_header" + "}" + ", " + "$" + "{" + "7:rows_per_page" + "}" + ", " + "$" + "{" + "8:options" + "}" + ")"),
        boost: 10
    },        {
        label: "from_json",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_json</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$pointer</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">JsonExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $path - string is internally turned into stream<br>@param ?string $pointer - if you want to iterate only results of a subtree, use a pointer, read more at https://github.com/halaxa/json-machine#parsing-a-subtree - @deprecate use withPointer method instead<br>@param null|Schema $schema - enforce schema on the extracted data - @deprecate use withSchema method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\JSON\\from_json(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:pointer" + "}" + ", " + "$" + "{" + "3:schema" + "}" + ")"),
        boost: 10
    },        {
        label: "from_json_lines",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_json_lines</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">JsonLinesExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Used to read from a JSON lines https://jsonlines.org/ formatted file.<br>@param Path|string $path - string is internally turned into stream
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\JSON\\from_json_lines(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },        {
        label: "from_meilisearch",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_meilisearch</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$params</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$index</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MeilisearchExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array{url: string, apiKey: string} $config<br>@param array{q: string, limit?: ?int, offset?: ?int, attributesToRetrieve?: ?array<string>, sort?: ?array<string>} $params
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Meilisearch\\from_meilisearch(" + "$" + "{" + "1:config" + "}" + ", " + "$" + "{" + "2:params" + "}" + ", " + "$" + "{" + "3:index" + "}" + ")"),
        boost: 10
    },        {
        label: "from_memory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_memory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Memory</span> <span class=\"fn-param\">$memory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MemoryExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_memory(" + "$" + "{" + "1:memory" + "}" + ")"),
        boost: 10
    },        {
        label: "from_parquet",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_parquet</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Parquet\\Options::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ByteOrder</span> <span class=\"fn-param\">$byte_order</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Parquet\\ByteOrder::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ParquetExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $path<br>@param array<string> $columns - list of columns to read from parquet file - @deprecated use \`withColumns\` method instead<br>@param Options $options - @deprecated use \`withOptions\` method instead<br>@param ByteOrder $byte_order - @deprecated use \`withByteOrder\` method instead<br>@param null|int $offset - @deprecated use \`withOffset\` method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Parquet\\from_parquet(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:columns" + "}" + ", " + "$" + "{" + "3:options" + "}" + ", " + "$" + "{" + "4:byte_order" + "}" + ", " + "$" + "{" + "5:offset" + "}" + ")"),
        boost: 10
    },        {
        label: "from_path_partitions",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_path_partitions</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PathPartitionsExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_path_partitions(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },        {
        label: "from_pipeline",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_pipeline</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Pipeline</span> <span class=\"fn-param\">$pipeline</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PipelineExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_pipeline(" + "$" + "{" + "1:pipeline" + "}" + ")"),
        boost: 10
    },        {
        label: "from_rows",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_rows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Rows</span> <span class=\"fn-param\">$rows</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RowsExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_rows(" + "$" + "{" + "1:rows" + "}" + ")"),
        boost: 10
    },        {
        label: "from_sequence_date_period",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_sequence_date_period</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateTimeInterface</span> <span class=\"fn-param\">$start</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateInterval</span> <span class=\"fn-param\">$interval</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateTimeInterface</span> <span class=\"fn-param\">$end</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SequenceExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_sequence_date_period(" + "$" + "{" + "1:entry_name" + "}" + ", " + "$" + "{" + "2:start" + "}" + ", " + "$" + "{" + "3:interval" + "}" + ", " + "$" + "{" + "4:end" + "}" + ", " + "$" + "{" + "5:options" + "}" + ")"),
        boost: 10
    },        {
        label: "from_sequence_date_period_recurrences",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_sequence_date_period_recurrences</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateTimeInterface</span> <span class=\"fn-param\">$start</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateInterval</span> <span class=\"fn-param\">$interval</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$recurrences</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SequenceExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_sequence_date_period_recurrences(" + "$" + "{" + "1:entry_name" + "}" + ", " + "$" + "{" + "2:start" + "}" + ", " + "$" + "{" + "3:interval" + "}" + ", " + "$" + "{" + "4:recurrences" + "}" + ", " + "$" + "{" + "5:options" + "}" + ")"),
        boost: 10
    },        {
        label: "from_sequence_number",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_sequence_number</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string|int|float</span> <span class=\"fn-param\">$start</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string|int|float</span> <span class=\"fn-param\">$end</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int|float</span> <span class=\"fn-param\">$step</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SequenceExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_sequence_number(" + "$" + "{" + "1:entry_name" + "}" + ", " + "$" + "{" + "2:start" + "}" + ", " + "$" + "{" + "3:end" + "}" + ", " + "$" + "{" + "4:step" + "}" + ")"),
        boost: 10
    },        {
        label: "from_static_http_requests",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_static_http_requests</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ClientInterface</span> <span class=\"fn-param\">$client</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">iterable</span> <span class=\"fn-param\">$requests</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PsrHttpClientStaticExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param iterable<RequestInterface> $requests
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Http\\from_static_http_requests(" + "$" + "{" + "1:client" + "}" + ", " + "$" + "{" + "2:requests" + "}" + ")"),
        boost: 10
    },        {
        label: "from_text",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_text</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TextExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $path
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Text\\from_text(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },        {
        label: "from_xml",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_xml</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$xml_node_path</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">XMLParserExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                     In order to iterate only over <element> nodes use \`from_xml($file)->withXMLNodePath(\'root/elements/element\')\`.<br> <root><br>   <elements><br>     <element></element><br>     <element></element><br>   <elements><br> </root><br> XML Node Path does not support attributes and it\'s not xpath, it is just a sequence<br> of node names separated with slash.<br>@param Path|string $path<br>@param string $xml_node_path - @deprecated use \`from_xml($file)->withXMLNodePath($xmlNodePath)\` method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\XML\\from_xml(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:xml_node_path" + "}" + ")"),
        boost: 10
    },        {
        label: "fstab",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">fstab</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Filesystem</span> <span class=\"fn-param\">$filesystems</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FilesystemTable</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a new filesystem table with given filesystems.<br>Filesystems can be also mounted later.<br>If no filesystems are provided, local filesystem is mounted.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\fstab(" + "$" + "{" + "1:filesystems" + "}" + ")"),
        boost: 10
    },        {
        label: "generate_random_int",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">generate_random_int</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$start</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">-9223372036854775808</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$end</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">9223372036854775807</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">NativePHPRandomValueGenerator</span> <span class=\"fn-param\">$generator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\NativePHPRandomValueGenerator::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">int</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\generate_random_int(" + "$" + "{" + "1:start" + "}" + ", " + "$" + "{" + "2:end" + "}" + ", " + "$" + "{" + "3:generator" + "}" + ")"),
        boost: 10
    },        {
        label: "generate_random_string",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">generate_random_string</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$length</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">32</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">NativePHPRandomValueGenerator</span> <span class=\"fn-param\">$generator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\NativePHPRandomValueGenerator::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\generate_random_string(" + "$" + "{" + "1:length" + "}" + ", " + "$" + "{" + "2:generator" + "}" + ")"),
        boost: 10
    },        {
        label: "get_type",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">get_type</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<mixed><br>@deprecated Please use \\Flow\\Types\\DSL\\get_type($value) instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\get_type(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "get_type",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">get_type</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<mixed>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\get_type(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "greatest",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">greatest</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$values</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Greatest</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\greatest(" + "$" + "{" + "1:values" + "}" + ")"),
        boost: 10
    },        {
        label: "hash",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">hash</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Algorithm</span> <span class=\"fn-param\">$algorithm</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Hash\\NativePHPHash::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Hash</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\hash(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:algorithm" + "}" + ")"),
        boost: 10
    },        {
        label: "hash_id_factory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">hash_id_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry_names</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IdFactory</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Elasticsearch\\hash_id_factory(" + "$" + "{" + "1:entry_names" + "}" + ")"),
        boost: 10
    },        {
        label: "html_element_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">html_element_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Dom\\HTMLElement|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?HTMLElement>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\html_element_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "html_element_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">html_element_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Definition<HTMLElement>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\html_element_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "html_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">html_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Dom\\HTMLDocument|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?HTMLDocument>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\html_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "html_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">html_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Definition<HTMLDocument>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\html_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "identical",
        type: "function",
        detail: "flow\u002Ddsl\u002Dcomparisons",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">identical</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Identical</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\identical(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },        {
        label: "ignore",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">ignore</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for save_mode_ignore().
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\ignore()"),
        boost: 10
    },        {
        label: "ignore_error_handler",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">ignore_error_handler</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IgnoreError</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\ignore_error_handler()"),
        boost: 10
    },        {
        label: "integer_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">integer_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?int>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\integer_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "integer_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">integer_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Definition<int>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\integer_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "int_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">int_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?int>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\int_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "int_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">int_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for \`int_schema\`.<br>@return Definition<int>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\int_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "is_type",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">is_type</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type|array</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string|Type<mixed>>|Type<mixed> $type<br>@param mixed $value
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\is_type(" + "$" + "{" + "1:type" + "}" + ", " + "$" + "{" + "2:value" + "}" + ")"),
        boost: 10
    },        {
        label: "is_valid_excel_sheet_name",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">is_valid_excel_sheet_name</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$sheet_name</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IsValidExcelSheetName</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Excel\\DSL\\is_valid_excel_sheet_name(" + "$" + "{" + "1:sheet_name" + "}" + ")"),
        boost: 10
    },        {
        label: "join_on",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">join_on</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Comparison|array</span> <span class=\"fn-param\">$comparisons</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$join_prefix</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Expression</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<\\Flow\\ETL\\Join\\Comparison|string>|Comparison $comparisons
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\join_on(" + "$" + "{" + "1:comparisons" + "}" + ", " + "$" + "{" + "2:join_prefix" + "}" + ")"),
        boost: 10
    },        {
        label: "json_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">json_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array|string|null</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param null|array<array-key, mixed>|string $data<br>@return Entry<?array<mixed>>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\json_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:data" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "json_object_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">json_object_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array|string|null</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param null|array<array-key, mixed>|string $data<br>@throws InvalidArgumentException<br>@return Entry<mixed>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\json_object_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:data" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "json_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">json_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Definition<string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\json_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "last",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">last</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Last</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\last(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "least",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">least</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$values</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Least</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\least(" + "$" + "{" + "1:values" + "}" + ")"),
        boost: 10
    },        {
        label: "limit",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtransformers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">limit</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Limit</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\limit(" + "$" + "{" + "1:limit" + "}" + ")"),
        boost: 10
    },        {
        label: "line_chart",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">line_chart</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference</span> <span class=\"fn-param\">$label</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">References</span> <span class=\"fn-param\">$datasets</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">LineChart</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\ChartJS\\line_chart(" + "$" + "{" + "1:label" + "}" + ", " + "$" + "{" + "2:datasets" + "}" + ")"),
        boost: 10
    },        {
        label: "list_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">list_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ListType</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param null|list<mixed> $value<br>@param ListType<T> $type<br>@return Entry<mixed>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\list_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:type" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "list_ref",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">list_ref</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ListFunctions</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\list_ref(" + "$" + "{" + "1:entry" + "}" + ")"),
        boost: 10
    },        {
        label: "list_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">list_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<list<T>> $type<br>@return Definition<list<T>>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\list_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:type" + "}" + ", " + "$" + "{" + "3:nullable" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "lit",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">lit</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Literal</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\lit(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "lower",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">lower</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToLower</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\lower(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "map_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">map_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$mapType</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template TKey of array-key<br>@template TValue<br>@param ?array<array-key, mixed> $value<br>@param Type<array<TKey, TValue>> $mapType<br>@return Entry<?array<TKey, TValue>>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\map_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:mapType" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "map_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">map_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template TKey of array-key<br>@template TValue<br>@param Type<array<TKey, TValue>> $type<br>@return Definition<array<TKey, TValue>>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\map_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:type" + "}" + ", " + "$" + "{" + "3:nullable" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "mask_columns",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtransformers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">mask_columns</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$mask</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;******&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MaskColumns</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<int, string> $columns
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\mask_columns(" + "$" + "{" + "1:columns" + "}" + ", " + "$" + "{" + "2:mask" + "}" + ")"),
        boost: 10
    },        {
        label: "match_cases",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">match_cases</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$cases</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$default</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MatchCases</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<MatchCondition> $cases
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\match_cases(" + "$" + "{" + "1:cases" + "}" + ", " + "$" + "{" + "2:default" + "}" + ")"),
        boost: 10
    },        {
        label: "match_condition",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">match_condition</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$condition</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$then</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MatchCondition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\match_condition(" + "$" + "{" + "1:condition" + "}" + ", " + "$" + "{" + "2:then" + "}" + ")"),
        boost: 10
    },        {
        label: "max",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">max</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Max</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\max(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "meilisearch_hits_to_rows",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">meilisearch_hits_to_rows</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">HitsIntoRowsTransformer</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Transforms Meilisearch results into clear Flow Rows.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Meilisearch\\meilisearch_hits_to_rows()"),
        boost: 10
    },        {
        label: "memory_filesystem",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">memory_filesystem</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MemoryFilesystem</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a new memory filesystem and writes data to it in memory.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\memory_filesystem()"),
        boost: 10
    },        {
        label: "min",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">min</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Min</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\min(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "mysql_insert_options",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">mysql_insert_options</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$skip_conflicts</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$upsert</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$update_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MySQLInsertOptions</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string> $update_columns
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\mysql_insert_options(" + "$" + "{" + "1:skip_conflicts" + "}" + ", " + "$" + "{" + "2:upsert" + "}" + ", " + "$" + "{" + "3:update_columns" + "}" + ")"),
        boost: 10
    },        {
        label: "native_local_filesystem",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">native_local_filesystem</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">NativeLocalFilesystem</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\native_local_filesystem()"),
        boost: 10
    },        {
        label: "not",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">not</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Not</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\not(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "now",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">now</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DateTimeZone|ScalarFunction</span> <span class=\"fn-param\">$time_zone</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">DateTimeZone::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Now</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\now(" + "$" + "{" + "1:time_zone" + "}" + ")"),
        boost: 10
    },        {
        label: "null_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">null_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    This functions is an alias for creating string entry from null.<br>The main difference between using this function an simply str_entry with second argument null<br>is that this function will also keep a note in the metadata that type might not be final.<br>For example when we need to guess column type from rows because schema was not provided,<br>and given column in the first row is null, it might still change once we get to the second row.<br>That metadata is used to determine if string_entry was created from null or not.<br>By design flow assumes when guessing column type that null would be a string (the most flexible type).<br>@return Entry<?string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\null_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "null_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">null_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Definition<string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\null_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "number_format",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">number_format</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int|float</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$decimals</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">2</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$decimal_separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;.&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$thousands_separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;,&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">NumberFormat</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\number_format(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:decimals" + "}" + ", " + "$" + "{" + "3:decimal_separator" + "}" + ", " + "$" + "{" + "4:thousands_separator" + "}" + ")"),
        boost: 10
    },        {
        label: "optional",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">optional</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Optional</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\optional(" + "$" + "{" + "1:function" + "}" + ")"),
        boost: 10
    },        {
        label: "overwrite",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">overwrite</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for save_mode_overwrite().
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\overwrite()"),
        boost: 10
    },        {
        label: "pagination_key_asc",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pagination_key_asc</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ParameterType|Type|string|int</span> <span class=\"fn-param\">$type</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Doctrine\\DBAL\\ParameterType::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Key</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\pagination_key_asc(" + "$" + "{" + "1:column" + "}" + ", " + "$" + "{" + "2:type" + "}" + ")"),
        boost: 10
    },        {
        label: "pagination_key_desc",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pagination_key_desc</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ParameterType|Type|string|int</span> <span class=\"fn-param\">$type</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Doctrine\\DBAL\\ParameterType::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Key</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\pagination_key_desc(" + "$" + "{" + "1:column" + "}" + ", " + "$" + "{" + "2:type" + "}" + ")"),
        boost: 10
    },        {
        label: "pagination_key_set",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pagination_key_set</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Key</span> <span class=\"fn-param\">$keys</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">KeySet</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\pagination_key_set(" + "$" + "{" + "1:keys" + "}" + ")"),
        boost: 10
    },        {
        label: "partition",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">partition</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Partition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\partition(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ")"),
        boost: 10
    },        {
        label: "partitions",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">partitions</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Partition</span> <span class=\"fn-param\">$partition</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Partitions</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\partitions(" + "$" + "{" + "1:partition" + "}" + ")"),
        boost: 10
    },        {
        label: "path",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">path</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options|array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Path</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Path supports glob patterns.<br>Examples:<br> - path(\'*.csv\') - any csv file in current directory<br> - path(\'/** / *.csv\') - any csv file in any subdirectory (remove empty spaces)<br> - path(\'/dir/partition=* /*.parquet\') - any parquet file in given partition directory.<br>Glob pattern is also supported by remote filesystems like Azure<br> - path(\'azure-blob://directory/*.csv\') - any csv file in given directory<br>@param array<string, null|bool|float|int|string|\\UnitEnum>|Path\\Options $options
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\path(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:options" + "}" + ")"),
        boost: 10
    },        {
        label: "path_memory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">path_memory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$path</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Path</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a path to php memory stream.<br>@param string $path - default = \'\' - path is used as an identifier in memory filesystem, so we can write multiple files to memory at once, each path is a new handle<br>@param null|array{\'stream\': \'memory\'|\'temp\'} $options - when nothing is provided, \'temp\' stream is used by default<br>@return Path
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\path_memory(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:options" + "}" + ")"),
        boost: 10
    },        {
        label: "path_real",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">path_real</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Path</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Resolve real path from given path.<br>@param array<string, null|bool|float|int|string|\\UnitEnum> $options
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\path_real(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:options" + "}" + ")"),
        boost: 10
    },        {
        label: "path_stdout",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">path_stdout</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Path</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a path to php stdout stream.<br>@param null|array{\'stream\': \'output\'|\'stderr\'|\'stdout\'} $options<br>@return Path
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\path_stdout(" + "$" + "{" + "1:options" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_count_modifier",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_count_modifier</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CountModifier</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a CountModifier that transforms a SELECT query into a COUNT query.<br>The original query is wrapped in: SELECT COUNT(*) FROM (...) AS _count_subq<br>ORDER BY and LIMIT/OFFSET are removed from the inner query.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_count_modifier()"),
        boost: 10
    },        {
        label: "pg_deparse",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_deparse</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ParsedQuery</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DeparseOptions</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Convert a ParsedQuery AST back to SQL string.<br>When called without options, returns the SQL as a simple string.<br>When called with DeparseOptions, applies formatting (pretty-printing, indentation, etc.).<br>@throws \\RuntimeException if deparsing fails
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_deparse(" + "$" + "{" + "1:query" + "}" + ", " + "$" + "{" + "2:options" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_deparse_options",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_deparse_options</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DeparseOptions</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create DeparseOptions for configuring SQL formatting.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_deparse_options()"),
        boost: 10
    },        {
        label: "pg_fingerprint",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_fingerprint</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Returns a fingerprint of the given SQL query.<br>Literal values are normalized so they won\'t affect the fingerprint.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_fingerprint(" + "$" + "{" + "1:sql" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_format",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_format</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DeparseOptions</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Parse and format SQL query with pretty printing.<br>This is a convenience function that parses SQL and returns it formatted.<br>@param string $sql The SQL query to format<br>@param null|DeparseOptions $options Formatting options (defaults to pretty-print enabled)<br>@throws \\RuntimeException if parsing or deparsing fails
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_format(" + "$" + "{" + "1:sql" + "}" + ", " + "$" + "{" + "2:options" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_keyset_column",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_keyset_column</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SortOrder</span> <span class=\"fn-param\">$order</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\PgQuery\\AST\\Transformers\\SortOrder::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">KeysetColumn</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a KeysetColumn for keyset pagination.<br>@param string $column Column name (can include table alias like \"u.id\")<br>@param SortOrder $order Sort order (ASC or DESC)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_keyset_column(" + "$" + "{" + "1:column" + "}" + ", " + "$" + "{" + "2:order" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_keyset_pagination",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_keyset_pagination</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$columns</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$cursor</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">KeysetPaginationModifier</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a KeysetPaginationModifier for cursor-based pagination.<br>Keyset pagination is more efficient than OFFSET for large datasets because it uses<br>indexed WHERE conditions instead of scanning and skipping rows.<br>@param int $limit Maximum number of rows to return<br>@param list<KeysetColumn> $columns Columns to use for keyset pagination (must match ORDER BY)<br>@param null|list<null|bool|float|int|string> $cursor Cursor values from the last row of previous page (null for first page)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_keyset_pagination(" + "$" + "{" + "1:limit" + "}" + ", " + "$" + "{" + "2:columns" + "}" + ", " + "$" + "{" + "3:cursor" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_keyset_pagination_config",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_keyset_pagination_config</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$columns</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$cursor</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">KeysetPaginationConfig</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a KeysetPaginationConfig for cursor-based pagination.<br>@param int $limit Maximum number of rows to return<br>@param list<KeysetColumn> $columns Columns to use for keyset pagination (must match ORDER BY)<br>@param null|list<null|bool|float|int|string> $cursor Cursor values from the last row of previous page (null for first page)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_keyset_pagination_config(" + "$" + "{" + "1:limit" + "}" + ", " + "$" + "{" + "2:columns" + "}" + ", " + "$" + "{" + "3:cursor" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_normalize",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_normalize</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Normalize SQL query by replacing literal values and named parameters with positional parameters.<br>WHERE id = :id will be changed into WHERE id = $1<br>WHERE id = 1 will be changed into WHERE id = $1.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_normalize(" + "$" + "{" + "1:sql" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_normalize_utility",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_normalize_utility</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Normalize utility SQL statements (DDL like CREATE, ALTER, DROP).<br>This handles DDL statements differently from pg_normalize() which is optimized for DML.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_normalize_utility(" + "$" + "{" + "1:sql" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_pagination",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_pagination</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PaginationModifier</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a PaginationModifier for offset-based pagination.<br>Applies LIMIT and OFFSET to the query. OFFSET without ORDER BY will throw an exception.<br>@param int $limit Maximum number of rows to return<br>@param int $offset Number of rows to skip (requires ORDER BY in query)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_pagination(" + "$" + "{" + "1:limit" + "}" + ", " + "$" + "{" + "2:offset" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_pagination_config",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_pagination_config</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PaginationConfig</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a PaginationConfig for offset-based pagination.<br>@param int $limit Maximum number of rows to return<br>@param int $offset Number of rows to skip (requires ORDER BY in query)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_pagination_config(" + "$" + "{" + "1:limit" + "}" + ", " + "$" + "{" + "2:offset" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_parse",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_parse</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ParsedQuery</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_parse(" + "$" + "{" + "1:sql" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_parser",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_parser</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Parser</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_parser()"),
        boost: 10
    },        {
        label: "pg_query_columns",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_query_columns</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ParsedQuery</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Columns</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Extract columns from a parsed SQL query.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_query_columns(" + "$" + "{" + "1:query" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_query_functions",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_query_functions</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ParsedQuery</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Functions</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Extract functions from a parsed SQL query.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_query_functions(" + "$" + "{" + "1:query" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_query_tables",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_query_tables</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ParsedQuery</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Tables</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Extract tables from a parsed SQL query.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_query_tables(" + "$" + "{" + "1:query" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_split",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_split</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">array</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Split string with multiple SQL statements into array of individual statements.<br>@return array<string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_split(" + "$" + "{" + "1:sql" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_summary",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_summary</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$truncateLimit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Generate a summary of parsed queries in protobuf format.<br>Useful for query monitoring and logging without full AST overhead.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_summary(" + "$" + "{" + "1:sql" + "}" + ", " + "$" + "{" + "2:options" + "}" + ", " + "$" + "{" + "3:truncateLimit" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_to_count_query",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_to_count_query</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Transform a SQL query into a COUNT query for pagination.<br>Wraps the query in: SELECT COUNT(*) FROM (...) AS _count_subq<br>Removes ORDER BY and LIMIT/OFFSET from the inner query.<br>@param string $sql The SQL query to transform<br>@return string The COUNT query
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_to_count_query(" + "$" + "{" + "1:sql" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_to_keyset_query",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_to_keyset_query</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$columns</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$cursor</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Transform a SQL query into a keyset (cursor-based) paginated query.<br>More efficient than OFFSET for large datasets - uses indexed WHERE conditions.<br>@param string $sql The SQL query to paginate (must have ORDER BY)<br>@param int $limit Maximum number of rows to return<br>@param list<KeysetColumn> $columns Columns for keyset pagination (must match ORDER BY)<br>@param null|list<null|bool|float|int|string> $cursor Values from last row of previous page (null for first page)<br>@return string The paginated SQL query
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_to_keyset_query(" + "$" + "{" + "1:sql" + "}" + ", " + "$" + "{" + "2:limit" + "}" + ", " + "$" + "{" + "3:columns" + "}" + ", " + "$" + "{" + "4:cursor" + "}" + ")"),
        boost: 10
    },        {
        label: "pg_to_paginated_query",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pg_to_paginated_query</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Transform a SQL query into a paginated query with LIMIT and OFFSET.<br>@param string $sql The SQL query to paginate<br>@param int $limit Maximum number of rows to return<br>@param int $offset Number of rows to skip (requires ORDER BY in query)<br>@return string The paginated SQL query
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PgQuery\\DSL\\pg_to_paginated_query(" + "$" + "{" + "1:sql" + "}" + ", " + "$" + "{" + "2:limit" + "}" + ", " + "$" + "{" + "3:offset" + "}" + ")"),
        boost: 10
    },        {
        label: "pie_chart",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pie_chart</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference</span> <span class=\"fn-param\">$label</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">References</span> <span class=\"fn-param\">$datasets</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PieChart</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\ChartJS\\pie_chart(" + "$" + "{" + "1:label" + "}" + ", " + "$" + "{" + "2:datasets" + "}" + ")"),
        boost: 10
    },        {
        label: "postgresql_insert_options",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">postgresql_insert_options</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$skip_conflicts</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$constraint</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$conflict_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$update_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSQLInsertOptions</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string> $conflict_columns<br>@param array<string> $update_columns
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\postgresql_insert_options(" + "$" + "{" + "1:skip_conflicts" + "}" + ", " + "$" + "{" + "2:constraint" + "}" + ", " + "$" + "{" + "3:conflict_columns" + "}" + ", " + "$" + "{" + "4:update_columns" + "}" + ")"),
        boost: 10
    },        {
        label: "postgresql_update_options",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">postgresql_update_options</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$primary_key_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$update_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSQLUpdateOptions</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string> $primary_key_columns<br>@param array<string> $update_columns
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\postgresql_update_options(" + "$" + "{" + "1:primary_key_columns" + "}" + ", " + "$" + "{" + "2:update_columns" + "}" + ")"),
        boost: 10
    },        {
        label: "print_rows",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">print_rows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Rows</span> <span class=\"fn-param\">$rows</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\print_rows(" + "$" + "{" + "1:rows" + "}" + ", " + "$" + "{" + "2:truncate" + "}" + ", " + "$" + "{" + "3:formatter" + "}" + ")"),
        boost: 10
    },        {
        label: "print_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">print_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Schema $schema<br>@deprecated Please use schema_to_ascii($schema) instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\print_schema(" + "$" + "{" + "1:schema" + "}" + ", " + "$" + "{" + "2:formatter" + "}" + ")"),
        boost: 10
    },        {
        label: "protocol",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">protocol</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$protocol</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Protocol</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\protocol(" + "$" + "{" + "1:protocol" + "}" + ")"),
        boost: 10
    },        {
        label: "random_string",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">random_string</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$length</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">RandomValueGenerator</span> <span class=\"fn-param\">$generator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\NativePHPRandomValueGenerator::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RandomString</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\random_string(" + "$" + "{" + "1:length" + "}" + ", " + "$" + "{" + "2:generator" + "}" + ")"),
        boost: 10
    },        {
        label: "rank",
        type: "function",
        detail: "flow\u002Ddsl\u002Dwindow\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">rank</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Rank</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\rank()"),
        boost: 10
    },        {
        label: "ref",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">ref</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EntryReference</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\ref(" + "$" + "{" + "1:entry" + "}" + ")"),
        boost: 10
    },        {
        label: "refs",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">refs</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">References</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\refs(" + "$" + "{" + "1:entries" + "}" + ")"),
        boost: 10
    },        {
        label: "regex",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regex</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$subject</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Regex</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\regex(" + "$" + "{" + "1:pattern" + "}" + ", " + "$" + "{" + "2:subject" + "}" + ", " + "$" + "{" + "3:flags" + "}" + ", " + "$" + "{" + "4:offset" + "}" + ")"),
        boost: 10
    },        {
        label: "regex_all",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regex_all</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$subject</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RegexAll</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\regex_all(" + "$" + "{" + "1:pattern" + "}" + ", " + "$" + "{" + "2:subject" + "}" + ", " + "$" + "{" + "3:flags" + "}" + ", " + "$" + "{" + "4:offset" + "}" + ")"),
        boost: 10
    },        {
        label: "regex_match",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regex_match</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$subject</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RegexMatch</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\regex_match(" + "$" + "{" + "1:pattern" + "}" + ", " + "$" + "{" + "2:subject" + "}" + ", " + "$" + "{" + "3:flags" + "}" + ", " + "$" + "{" + "4:offset" + "}" + ")"),
        boost: 10
    },        {
        label: "regex_match_all",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regex_match_all</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$subject</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RegexMatchAll</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\regex_match_all(" + "$" + "{" + "1:pattern" + "}" + ", " + "$" + "{" + "2:subject" + "}" + ", " + "$" + "{" + "3:flags" + "}" + ", " + "$" + "{" + "4:offset" + "}" + ")"),
        boost: 10
    },        {
        label: "regex_replace",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regex_replace</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$replacement</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$subject</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int|null</span> <span class=\"fn-param\">$limit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RegexReplace</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\regex_replace(" + "$" + "{" + "1:pattern" + "}" + ", " + "$" + "{" + "2:replacement" + "}" + ", " + "$" + "{" + "3:subject" + "}" + ", " + "$" + "{" + "4:limit" + "}" + ")"),
        boost: 10
    },        {
        label: "rename_replace",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtransformers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">rename_replace</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array|string</span> <span class=\"fn-param\">$search</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array|string</span> <span class=\"fn-param\">$replace</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RenameReplaceEntryStrategy</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string>|string $search<br>@param array<string>|string $replace
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\rename_replace(" + "$" + "{" + "1:search" + "}" + ", " + "$" + "{" + "2:replace" + "}" + ")"),
        boost: 10
    },        {
        label: "rename_style",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtransformers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">rename_style</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">StringStyles|StringStyles</span> <span class=\"fn-param\">$style</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RenameCaseEntryStrategy</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\rename_style(" + "$" + "{" + "1:style" + "}" + ")"),
        boost: 10
    },        {
        label: "retry_any_throwable",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">retry_any_throwable</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AnyThrowable</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\retry_any_throwable(" + "$" + "{" + "1:limit" + "}" + ")"),
        boost: 10
    },        {
        label: "retry_on_exception_types",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">retry_on_exception_types</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$exception_types</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OnExceptionTypes</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<class-string<\\Throwable>> $exception_types
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\retry_on_exception_types(" + "$" + "{" + "1:exception_types" + "}" + ", " + "$" + "{" + "2:limit" + "}" + ")"),
        boost: 10
    },        {
        label: "round",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">round</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int|float</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$precision</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">2</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$mode</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Round</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\round(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:precision" + "}" + ", " + "$" + "{" + "3:mode" + "}" + ")"),
        boost: 10
    },        {
        label: "row",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">row</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Entry</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Row</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Entry<mixed> ...$entry
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\row(" + "$" + "{" + "1:entry" + "}" + ")"),
        boost: 10
    },        {
        label: "rows",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">rows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Row</span> <span class=\"fn-param\">$row</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Rows</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\rows(" + "$" + "{" + "1:row" + "}" + ")"),
        boost: 10
    },        {
        label: "rows_partitioned",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">rows_partitioned</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$rows</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Partitions|array</span> <span class=\"fn-param\">$partitions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Rows</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<Row> $rows<br>@param array<\\Flow\\Filesystem\\Partition|string>|Partitions $partitions
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\rows_partitioned(" + "$" + "{" + "1:rows" + "}" + ", " + "$" + "{" + "2:partitions" + "}" + ")"),
        boost: 10
    },        {
        label: "row_number",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">row_number</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RowNumber</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\row_number()"),
        boost: 10
    },        {
        label: "sanitize",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sanitize</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$placeholder</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;*&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int|null</span> <span class=\"fn-param\">$skipCharacters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Sanitize</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\sanitize(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:placeholder" + "}" + ", " + "$" + "{" + "3:skipCharacters" + "}" + ")"),
        boost: 10
    },        {
        label: "save_mode_append",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">save_mode_append</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\save_mode_append()"),
        boost: 10
    },        {
        label: "save_mode_exception_if_exists",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">save_mode_exception_if_exists</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\save_mode_exception_if_exists()"),
        boost: 10
    },        {
        label: "save_mode_ignore",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">save_mode_ignore</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\save_mode_ignore()"),
        boost: 10
    },        {
        label: "save_mode_overwrite",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">save_mode_overwrite</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\save_mode_overwrite()"),
        boost: 10
    },        {
        label: "schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Definition</span> <span class=\"fn-param\">$definitions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Schema</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Definition<mixed> ...$definitions<br>@return Schema
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema(" + "$" + "{" + "1:definitions" + "}" + ")"),
        boost: 10
    },        {
        label: "schema_evolving_validator",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_evolving_validator</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EvolvingValidator</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_evolving_validator()"),
        boost: 10
    },        {
        label: "schema_from_json",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_from_json</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Schema</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Schema
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_from_json(" + "$" + "{" + "1:schema" + "}" + ")"),
        boost: 10
    },        {
        label: "schema_from_parquet",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_from_parquet</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Schema</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Parquet\\schema_from_parquet(" + "$" + "{" + "1:schema" + "}" + ")"),
        boost: 10
    },        {
        label: "schema_metadata",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_metadata</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Metadata</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string, array<bool|float|int|string>|bool|float|int|string> $metadata
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_metadata(" + "$" + "{" + "1:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "schema_selective_validator",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_selective_validator</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SelectiveValidator</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_selective_validator()"),
        boost: 10
    },        {
        label: "schema_strict_validator",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_strict_validator</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StrictValidator</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_strict_validator()"),
        boost: 10
    },        {
        label: "schema_to_ascii",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_to_ascii</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Schema $schema
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_to_ascii(" + "$" + "{" + "1:schema" + "}" + ", " + "$" + "{" + "2:formatter" + "}" + ")"),
        boost: 10
    },        {
        label: "schema_to_json",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_to_json</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$pretty</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Schema $schema
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_to_json(" + "$" + "{" + "1:schema" + "}" + ", " + "$" + "{" + "2:pretty" + "}" + ")"),
        boost: 10
    },        {
        label: "schema_to_parquet",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_to_parquet</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Schema</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Parquet\\schema_to_parquet(" + "$" + "{" + "1:schema" + "}" + ")"),
        boost: 10
    },        {
        label: "schema_to_php",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_to_php</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ValueFormatter</span> <span class=\"fn-param\">$valueFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Schema\\Formatter\\PHPFormatter\\ValueFormatter::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">TypeFormatter</span> <span class=\"fn-param\">$typeFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Schema\\Formatter\\PHPFormatter\\TypeFormatter::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Schema $schema
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_to_php(" + "$" + "{" + "1:schema" + "}" + ", " + "$" + "{" + "2:valueFormatter" + "}" + ", " + "$" + "{" + "3:typeFormatter" + "}" + ")"),
        boost: 10
    },        {
        label: "schema_validate",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_validate</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$expected</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$given</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaValidator</span> <span class=\"fn-param\">$validator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Schema\\Validator\\StrictValidator::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Schema $expected<br>@param Schema $given
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_validate(" + "$" + "{" + "1:expected" + "}" + ", " + "$" + "{" + "2:given" + "}" + ", " + "$" + "{" + "3:validator" + "}" + ")"),
        boost: 10
    },        {
        label: "select",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtransformers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">select</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Select</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\select(" + "$" + "{" + "1:entries" + "}" + ")"),
        boost: 10
    },        {
        label: "size",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">size</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Size</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\size(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "skip_rows_handler",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">skip_rows_handler</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SkipRows</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\skip_rows_handler()"),
        boost: 10
    },        {
        label: "split",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">split</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$separator</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$limit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">9223372036854775807</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Split</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\split(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:separator" + "}" + ", " + "$" + "{" + "3:limit" + "}" + ")"),
        boost: 10
    },        {
        label: "sprintf",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sprintf</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$format</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string|int|float|null</span> <span class=\"fn-param\">$args</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Sprintf</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\sprintf(" + "$" + "{" + "1:format" + "}" + ", " + "$" + "{" + "2:args" + "}" + ")"),
        boost: 10
    },        {
        label: "sqlite_insert_options",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sqlite_insert_options</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$skip_conflicts</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$conflict_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$update_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SqliteInsertOptions</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string> $conflict_columns<br>@param array<string> $update_columns
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\sqlite_insert_options(" + "$" + "{" + "1:skip_conflicts" + "}" + ", " + "$" + "{" + "2:conflict_columns" + "}" + ", " + "$" + "{" + "3:update_columns" + "}" + ")"),
        boost: 10
    },        {
        label: "stdout_filesystem",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">stdout_filesystem</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StdOutFilesystem</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Write-only filesystem useful when we just want to write the output to stdout.<br>The main use case is for streaming datasets over http.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\stdout_filesystem()"),
        boost: 10
    },        {
        label: "string_agg",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">string_agg</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;, &#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SortOrder</span> <span class=\"fn-param\">$sort</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringAggregate</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\string_agg(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:separator" + "}" + ", " + "$" + "{" + "3:sort" + "}" + ")"),
        boost: 10
    },        {
        label: "string_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">string_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\string_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "string_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">string_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Definition<string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\string_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "structure_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">structure_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param ?array<string, mixed> $value<br>@param Type<array<string, T>> $type<br>@return Entry<?array<string, T>>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\structure_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:type" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "structure_ref",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">structure_ref</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StructureFunctions</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\structure_ref(" + "$" + "{" + "1:entry" + "}" + ")"),
        boost: 10
    },        {
        label: "structure_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">structure_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<T> $type<br>@return Definition<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\structure_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:type" + "}" + ", " + "$" + "{" + "3:nullable" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "struct_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">struct_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param ?array<string, mixed> $value<br>@param Type<array<string, T>> $type<br>@return Entry<?array<string, T>>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\struct_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:type" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "struct_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">struct_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<T> $type<br>@return Definition<T><br>@deprecated Use \`structure_schema()\` instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\struct_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:type" + "}" + ", " + "$" + "{" + "3:nullable" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "str_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">str_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\str_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "str_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">str_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for \`string_schema\`.<br>@return Definition<string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\str_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "sum",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sum</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Sum</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\sum(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "table_schema_to_flow_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">table_schema_to_flow_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Table</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types_map</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Schema</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Converts a Doctrine\\DBAL\\Schema\\Table to a Flow\\ETL\\Schema.<br>@param array<class-string<\\Flow\\Types\\Type<mixed>>, class-string<\\Doctrine\\DBAL\\Types\\Type>> $types_map<br>@return Schema
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\table_schema_to_flow_schema(" + "$" + "{" + "1:table" + "}" + ", " + "$" + "{" + "2:types_map" + "}" + ")"),
        boost: 10
    },        {
        label: "throw_error_handler",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">throw_error_handler</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ThrowError</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\throw_error_handler()"),
        boost: 10
    },        {
        label: "time_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">time_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateInterval|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?\\DateInterval>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\time_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "time_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">time_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Definition<\\DateInterval>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\time_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "to_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_array</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$array</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Convert rows to an array and store them in passed array variable.<br>@param array<array-key, mixed> $array<br>@param-out array<array<mixed>> $array
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_array(" + "$" + "{" + "1:array" + "}" + ")"),
        boost: 10
    },        {
        label: "to_avro",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_avro</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AvroLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\Adapter\\Avro\\to_avro(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:schema" + "}" + ")"),
        boost: 10
    },        {
        label: "to_branch",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_branch</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$condition</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Loader</span> <span class=\"fn-param\">$loader</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BranchingLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_branch(" + "$" + "{" + "1:condition" + "}" + ", " + "$" + "{" + "2:loader" + "}" + ")"),
        boost: 10
    },        {
        label: "to_callable",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_callable</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">callable</span> <span class=\"fn-param\">$callable</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CallbackLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_callable(" + "$" + "{" + "1:callable" + "}" + ")"),
        boost: 10
    },        {
        label: "to_chartjs",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_chartjs</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Chart</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ChartJSLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\ChartJS\\to_chartjs(" + "$" + "{" + "1:type" + "}" + ")"),
        boost: 10
    },        {
        label: "to_chartjs_file",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_chartjs_file</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Chart</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Path|string|null</span> <span class=\"fn-param\">$output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Path|string|null</span> <span class=\"fn-param\">$template</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ChartJSLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Chart $type<br>@param null|Path|string $output - @deprecated use $loader->withOutputPath() instead<br>@param null|Path|string $template - @deprecated use $loader->withTemplate() instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\ChartJS\\to_chartjs_file(" + "$" + "{" + "1:type" + "}" + ", " + "$" + "{" + "2:output" + "}" + ", " + "$" + "{" + "3:template" + "}" + ")"),
        boost: 10
    },        {
        label: "to_chartjs_var",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_chartjs_var</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Chart</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$output</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ChartJSLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Chart $type<br>@param array<array-key, mixed> $output - @deprecated use $loader->withOutputVar() instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\ChartJS\\to_chartjs_var(" + "$" + "{" + "1:type" + "}" + ", " + "$" + "{" + "2:output" + "}" + ")"),
        boost: 10
    },        {
        label: "to_csv",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_csv</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$uri</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$with_header</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;,&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$enclosure</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;\\&quot;&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$escape</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;\\\\&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$new_line_separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;\\n&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$datetime_format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;Y-m-d\\\\TH:i:sP&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CSVLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $uri<br>@param bool $with_header - @deprecated use $loader->withHeader() instead<br>@param string $separator - @deprecated use $loader->withSeparator() instead<br>@param string $enclosure - @deprecated use $loader->withEnclosure() instead<br>@param string $escape - @deprecated use $loader->withEscape() instead<br>@param string $new_line_separator - @deprecated use $loader->withNewLineSeparator() instead<br>@param string $datetime_format - @deprecated use $loader->withDateTimeFormat() instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\CSV\\to_csv(" + "$" + "{" + "1:uri" + "}" + ", " + "$" + "{" + "2:with_header" + "}" + ", " + "$" + "{" + "3:separator" + "}" + ", " + "$" + "{" + "4:enclosure" + "}" + ", " + "$" + "{" + "5:escape" + "}" + ", " + "$" + "{" + "6:new_line_separator" + "}" + ", " + "$" + "{" + "7:datetime_format" + "}" + ")"),
        boost: 10
    },        {
        label: "to_date",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_date</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;Y-m-d&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|DateTimeZone</span> <span class=\"fn-param\">$timeZone</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">DateTimeZone::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToDate</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_date(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:format" + "}" + ", " + "$" + "{" + "3:timeZone" + "}" + ")"),
        boost: 10
    },        {
        label: "to_date_time",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_date_time</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;Y-m-d H:i:s&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|DateTimeZone</span> <span class=\"fn-param\">$timeZone</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">DateTimeZone::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToDateTime</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_date_time(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:format" + "}" + ", " + "$" + "{" + "3:timeZone" + "}" + ")"),
        boost: 10
    },        {
        label: "to_dbal_schema_table",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_dbal_schema_table</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$table_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$table_options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types_map</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Table</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Converts a Flow\\ETL\\Schema to a Doctrine\\DBAL\\Schema\\Table.<br>@param Schema $schema<br>@param array<array-key, mixed> $table_options<br>@param array<class-string<\\Flow\\Types\\Type<mixed>>, class-string<\\Doctrine\\DBAL\\Types\\Type>> $types_map
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\to_dbal_schema_table(" + "$" + "{" + "1:schema" + "}" + ", " + "$" + "{" + "2:table_name" + "}" + ", " + "$" + "{" + "3:table_options" + "}" + ", " + "$" + "{" + "4:types_map" + "}" + ")"),
        boost: 10
    },        {
        label: "to_dbal_table_delete",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_dbal_table_delete</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection|array</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Delete rows from database table based on the provided data.<br>In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().<br>@param array<string, mixed>|Connection $connection<br>@throws InvalidArgumentException
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\to_dbal_table_delete(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:table" + "}" + ")"),
        boost: 10
    },        {
        label: "to_dbal_table_insert",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_dbal_table_insert</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection|array</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">InsertOptions</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Insert new rows into a database table.<br>Insert can also be used as an upsert with the help of InsertOptions.<br>InsertOptions are platform specific, so please choose the right one for your database.<br> - MySQLInsertOptions<br> - PostgreSQLInsertOptions<br> - SqliteInsertOptions<br>In order to control the size of the single insert, use DataFrame::chunkSize() method just before calling DataFrame::load().<br>@param array<string, mixed>|Connection $connection<br>@throws InvalidArgumentException
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\to_dbal_table_insert(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:table" + "}" + ", " + "$" + "{" + "3:options" + "}" + ")"),
        boost: 10
    },        {
        label: "to_dbal_table_update",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_dbal_table_update</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection|array</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">UpdateOptions</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                     Update existing rows in database.<br> In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().<br>@param array<string, mixed>|Connection $connection<br>@throws InvalidArgumentException
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\to_dbal_table_update(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:table" + "}" + ", " + "$" + "{" + "3:options" + "}" + ")"),
        boost: 10
    },        {
        label: "to_dbal_transaction",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_dbal_transaction</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection|array</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Loader</span> <span class=\"fn-param\">$loaders</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TransactionalDbalLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Execute multiple loaders within a database transaction.<br>Each batch of rows will be processed in its own transaction.<br>If any loader fails, the entire batch will be rolled back.<br>@param array<string, mixed>|Connection $connection<br>@param Loader ...$loaders - Loaders to execute within the transaction<br>@throws InvalidArgumentException
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\to_dbal_transaction(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:loaders" + "}" + ")"),
        boost: 10
    },        {
        label: "to_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">EntryFactory</span> <span class=\"fn-param\">$entryFactory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<mixed> $data<br>@return Entry<mixed>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:data" + "}" + ", " + "$" + "{" + "3:entryFactory" + "}" + ")"),
        boost: 10
    },        {
        label: "to_es_bulk_index",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_es_bulk_index</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$index</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">IdFactory</span> <span class=\"fn-param\">$id_factory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ElasticsearchLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html.<br>In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().<br>@param array{<br> hosts?: array<string>,<br> connectionParams?: array<mixed>,<br> retries?: int,<br> sniffOnStart?: bool,<br> sslCert?: array<string>,<br> sslKey?: array<string>,<br> sslVerification?: bool|string,<br> elasticMetaHeader?: bool,<br> includePortInHostHeader?: bool<br>} $config<br>@param string $index<br>@param IdFactory $id_factory<br>@param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html - @deprecated use withParameters method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Elasticsearch\\to_es_bulk_index(" + "$" + "{" + "1:config" + "}" + ", " + "$" + "{" + "2:index" + "}" + ", " + "$" + "{" + "3:id_factory" + "}" + ", " + "$" + "{" + "4:parameters" + "}" + ")"),
        boost: 10
    },        {
        label: "to_es_bulk_update",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_es_bulk_update</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$index</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">IdFactory</span> <span class=\"fn-param\">$id_factory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ElasticsearchLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                     https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html.<br>In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().<br>@param array{<br> hosts?: array<string>,<br> connectionParams?: array<mixed>,<br> retries?: int,<br> sniffOnStart?: bool,<br> sslCert?: array<string>,<br> sslKey?: array<string>,<br> sslVerification?: bool|string,<br> elasticMetaHeader?: bool,<br> includePortInHostHeader?: bool<br>} $config<br>@param string $index<br>@param IdFactory $id_factory<br>@param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html - @deprecated use withParameters method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Elasticsearch\\to_es_bulk_update(" + "$" + "{" + "1:config" + "}" + ", " + "$" + "{" + "2:index" + "}" + ", " + "$" + "{" + "3:id_factory" + "}" + ", " + "$" + "{" + "4:parameters" + "}" + ")"),
        boost: 10
    },        {
        label: "to_json",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_json</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">4194304</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$date_time_format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;Y-m-d\\\\TH:i:sP&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$put_rows_in_new_lines</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">JsonLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $path<br>@param int $flags - PHP JSON Flags - @deprecate use withFlags method instead<br>@param string $date_time_format - format for DateTimeInterface::format() - @deprecate use withDateTimeFormat method instead<br>@param bool $put_rows_in_new_lines - if you want to put each row in a new line - @deprecate use withRowsInNewLines method instead<br>@return JsonLoader
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\JSON\\to_json(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:flags" + "}" + ", " + "$" + "{" + "3:date_time_format" + "}" + ", " + "$" + "{" + "4:put_rows_in_new_lines" + "}" + ")"),
        boost: 10
    },        {
        label: "to_json_lines",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_json_lines</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">JsonLinesLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Used to write to a JSON lines https://jsonlines.org/ formatted file.<br>@param Path|string $path<br>@return JsonLinesLoader
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\JSON\\to_json_lines(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },        {
        label: "to_meilisearch_bulk_index",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_meilisearch_bulk_index</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$index</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Loader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array{url: string, apiKey: string, httpClient: ?ClientInterface} $config
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Meilisearch\\to_meilisearch_bulk_index(" + "$" + "{" + "1:config" + "}" + ", " + "$" + "{" + "2:index" + "}" + ")"),
        boost: 10
    },        {
        label: "to_meilisearch_bulk_update",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_meilisearch_bulk_update</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$index</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Loader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array{url: string, apiKey: string, httpClient: ?ClientInterface} $config
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Meilisearch\\to_meilisearch_bulk_update(" + "$" + "{" + "1:config" + "}" + ", " + "$" + "{" + "2:index" + "}" + ")"),
        boost: 10
    },        {
        label: "to_memory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_memory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Memory</span> <span class=\"fn-param\">$memory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MemoryLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_memory(" + "$" + "{" + "1:memory" + "}" + ")"),
        boost: 10
    },        {
        label: "to_output",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_output</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">20</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Output</span> <span class=\"fn-param\">$output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Loader\\StreamLoader\\Output::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Formatter\\AsciiTableFormatter::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$schemaFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Row\\Formatter\\ASCIISchemaFormatter::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StreamLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_output(" + "$" + "{" + "1:truncate" + "}" + ", " + "$" + "{" + "2:output" + "}" + ", " + "$" + "{" + "3:formatter" + "}" + ", " + "$" + "{" + "4:schemaFormatter" + "}" + ")"),
        boost: 10
    },        {
        label: "to_parquet",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_parquet</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Compressions</span> <span class=\"fn-param\">$compressions</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Parquet\\ParquetFile\\Compressions::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ParquetLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $path<br>@param null|Options $options - @deprecated use \`withOptions\` method instead<br>@param Compressions $compressions - @deprecated use \`withCompressions\` method instead<br>@param null|Schema $schema - @deprecated use \`withSchema\` method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Parquet\\to_parquet(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:options" + "}" + ", " + "$" + "{" + "3:compressions" + "}" + ", " + "$" + "{" + "4:schema" + "}" + ")"),
        boost: 10
    },        {
        label: "to_stderr",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_stderr</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">20</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Output</span> <span class=\"fn-param\">$output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Loader\\StreamLoader\\Output::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Formatter\\AsciiTableFormatter::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$schemaFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Row\\Formatter\\ASCIISchemaFormatter::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StreamLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_stderr(" + "$" + "{" + "1:truncate" + "}" + ", " + "$" + "{" + "2:output" + "}" + ", " + "$" + "{" + "3:formatter" + "}" + ", " + "$" + "{" + "4:schemaFormatter" + "}" + ")"),
        boost: 10
    },        {
        label: "to_stdout",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_stdout</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">20</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Output</span> <span class=\"fn-param\">$output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Loader\\StreamLoader\\Output::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Formatter\\AsciiTableFormatter::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$schemaFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Row\\Formatter\\ASCIISchemaFormatter::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StreamLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_stdout(" + "$" + "{" + "1:truncate" + "}" + ", " + "$" + "{" + "2:output" + "}" + ", " + "$" + "{" + "3:formatter" + "}" + ", " + "$" + "{" + "4:schemaFormatter" + "}" + ")"),
        boost: 10
    },        {
        label: "to_stream",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_stream</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$uri</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">20</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Output</span> <span class=\"fn-param\">$output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Loader\\StreamLoader\\Output::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$mode</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;w&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Formatter\\AsciiTableFormatter::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$schemaFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Row\\Formatter\\ASCIISchemaFormatter::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StreamLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_stream(" + "$" + "{" + "1:uri" + "}" + ", " + "$" + "{" + "2:truncate" + "}" + ", " + "$" + "{" + "3:output" + "}" + ", " + "$" + "{" + "4:mode" + "}" + ", " + "$" + "{" + "5:formatter" + "}" + ", " + "$" + "{" + "6:schemaFormatter" + "}" + ")"),
        boost: 10
    },        {
        label: "to_text",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_text</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$new_line_separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;\\n&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Loader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $path<br>@param string $new_line_separator - default PHP_EOL - @deprecated use withNewLineSeparator method instead<br>@return Loader
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Text\\to_text(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:new_line_separator" + "}" + ")"),
        boost: 10
    },        {
        label: "to_timezone",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_timezone</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|DateTimeInterface</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|DateTimeZone|string</span> <span class=\"fn-param\">$timeZone</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToTimeZone</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_timezone(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:timeZone" + "}" + ")"),
        boost: 10
    },        {
        label: "to_transformation",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_transformation</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Transformer|Transformation</span> <span class=\"fn-param\">$transformer</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Loader</span> <span class=\"fn-param\">$loader</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TransformerLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_transformation(" + "$" + "{" + "1:transformer" + "}" + ", " + "$" + "{" + "2:loader" + "}" + ")"),
        boost: 10
    },        {
        label: "to_xml",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_xml</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$root_element_name</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;rows&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$row_element_name</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;row&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$attribute_prefix</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;_&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$date_time_format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;Y-m-d\\\\TH:i:s.uP&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">XMLWriter</span> <span class=\"fn-param\">$xml_writer</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Adapter\\XML\\XMLWriter\\DOMDocumentWriter::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">XMLLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $path<br>@param string $root_element_name - @deprecated use \`withRootElementName()\` method instead<br>@param string $row_element_name - @deprecated use \`withRowElementName()\` method instead<br>@param string $attribute_prefix - @deprecated use \`withAttributePrefix()\` method instead<br>@param string $date_time_format - @deprecated use \`withDateTimeFormat()\` method instead<br>@param XMLWriter $xml_writer
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\XML\\to_xml(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:root_element_name" + "}" + ", " + "$" + "{" + "3:row_element_name" + "}" + ", " + "$" + "{" + "4:attribute_prefix" + "}" + ", " + "$" + "{" + "5:date_time_format" + "}" + ", " + "$" + "{" + "6:xml_writer" + "}" + ")"),
        boost: 10
    },        {
        label: "types",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">types</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$types</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Types</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<T> ...$types<br>@return Types<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\types(" + "$" + "{" + "1:types" + "}" + ")"),
        boost: 10
    },        {
        label: "type_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_array</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<array<mixed>>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_array()"),
        boost: 10
    },        {
        label: "type_boolean",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_boolean</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<bool>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_boolean()"),
        boost: 10
    },        {
        label: "type_callable",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_callable</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<callable>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_callable()"),
        boost: 10
    },        {
        label: "type_class_string",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_class_string</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$class</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T of object<br>@param null|class-string<T> $class<br>@return ($class is null ? Type<class-string> : Type<class-string<T>>)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_class_string(" + "$" + "{" + "1:class" + "}" + ")"),
        boost: 10
    },        {
        label: "type_date",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_date</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @deprecated please use \\Flow\\Types\\DSL\\type_date() : DateType<br>@return Type<\\DateTimeInterface>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\type_date()"),
        boost: 10
    },        {
        label: "type_date",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_date</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<\\DateTimeInterface>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_date()"),
        boost: 10
    },        {
        label: "type_datetime",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_datetime</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<\\DateTimeInterface>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_datetime()"),
        boost: 10
    },        {
        label: "type_enum",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_enum</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$class</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T of UnitEnum<br>@param class-string<T> $class<br>@return Type<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_enum(" + "$" + "{" + "1:class" + "}" + ")"),
        boost: 10
    },        {
        label: "type_equals",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_equals</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Type<mixed> $left<br>@param Type<mixed> $right
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_equals(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },        {
        label: "type_float",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_float</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<float>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_float()"),
        boost: 10
    },        {
        label: "type_from_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_from_array</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string, mixed> $data<br>@return Type<mixed>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_from_array(" + "$" + "{" + "1:data" + "}" + ")"),
        boost: 10
    },        {
        label: "type_html",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_html</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<HTMLDocument>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_html()"),
        boost: 10
    },        {
        label: "type_html_element",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_html_element</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<HTMLElement>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_html_element()"),
        boost: 10
    },        {
        label: "type_instance_of",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_instance_of</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$class</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T of object<br>@param class-string<T> $class<br>@return Type<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_instance_of(" + "$" + "{" + "1:class" + "}" + ")"),
        boost: 10
    },        {
        label: "type_int",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_int</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @deprecated please use \\Flow\\Types\\DSL\\type_integer() : IntegerType<br>@return Type<int>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\type_int()"),
        boost: 10
    },        {
        label: "type_integer",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_integer</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<int>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_integer()"),
        boost: 10
    },        {
        label: "type_intersection",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_intersection</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$first</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$second</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$types</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<T> $first<br>@param Type<T> $second<br>@param Type<T> ...$types<br>@return Type<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_intersection(" + "$" + "{" + "1:first" + "}" + ", " + "$" + "{" + "2:second" + "}" + ", " + "$" + "{" + "3:types" + "}" + ")"),
        boost: 10
    },        {
        label: "type_is",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_is</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$typeClass</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<T> $type<br>@param class-string<Type<mixed>> $typeClass
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_is(" + "$" + "{" + "1:type" + "}" + ", " + "$" + "{" + "2:typeClass" + "}" + ")"),
        boost: 10
    },        {
        label: "type_is_any",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_is_any</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$typeClass</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$typeClasses</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<T> $type<br>@param class-string<Type<mixed>> $typeClass<br>@param class-string<Type<mixed>> ...$typeClasses
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_is_any(" + "$" + "{" + "1:type" + "}" + ", " + "$" + "{" + "2:typeClass" + "}" + ", " + "$" + "{" + "3:typeClasses" + "}" + ")"),
        boost: 10
    },        {
        label: "type_is_nullable",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_is_nullable</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<T> $type
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_is_nullable(" + "$" + "{" + "1:type" + "}" + ")"),
        boost: 10
    },        {
        label: "type_json",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_json</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_json()"),
        boost: 10
    },        {
        label: "type_list",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_list</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$element</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ListType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<T> $element<br>@return ListType<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_list(" + "$" + "{" + "1:element" + "}" + ")"),
        boost: 10
    },        {
        label: "type_literal",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_literal</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string|int|float|bool</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">LiteralType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T of bool|float|int|string<br>@param T $value<br>@return LiteralType<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_literal(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "type_map",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_map</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$key_type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$value_type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template TKey of array-key<br>@template TValue<br>@param Type<TKey> $key_type<br>@param Type<TValue> $value_type<br>@return Type<array<TKey, TValue>>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_map(" + "$" + "{" + "1:key_type" + "}" + ", " + "$" + "{" + "2:value_type" + "}" + ")"),
        boost: 10
    },        {
        label: "type_mixed",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_mixed</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<mixed>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_mixed()"),
        boost: 10
    },        {
        label: "type_non_empty_string",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_non_empty_string</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<non-empty-string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_non_empty_string()"),
        boost: 10
    },        {
        label: "type_null",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_null</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<null>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_null()"),
        boost: 10
    },        {
        label: "type_numeric_string",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_numeric_string</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<numeric-string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_numeric_string()"),
        boost: 10
    },        {
        label: "type_object",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_object</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<object>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_object()"),
        boost: 10
    },        {
        label: "type_optional",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_optional</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<T> $type<br>@return Type<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_optional(" + "$" + "{" + "1:type" + "}" + ")"),
        boost: 10
    },        {
        label: "type_positive_integer",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_positive_integer</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<int<0, max>>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_positive_integer()"),
        boost: 10
    },        {
        label: "type_resource",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_resource</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<resource>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_resource()"),
        boost: 10
    },        {
        label: "type_scalar",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_scalar</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<bool|float|int|string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_scalar()"),
        boost: 10
    },        {
        label: "type_string",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_string</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_string()"),
        boost: 10
    },        {
        label: "type_structure",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_structure</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$elements</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$optional_elements</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$allow_extra</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param array<string, Type<T>> $elements<br>@param array<string, Type<T>> $optional_elements<br>@return Type<array<string, T>>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_structure(" + "$" + "{" + "1:elements" + "}" + ", " + "$" + "{" + "2:optional_elements" + "}" + ", " + "$" + "{" + "3:allow_extra" + "}" + ")"),
        boost: 10
    },        {
        label: "type_time",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_time</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<\\DateInterval>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_time()"),
        boost: 10
    },        {
        label: "type_time_zone",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_time_zone</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<\\DateTimeZone>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_time_zone()"),
        boost: 10
    },        {
        label: "type_union",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_union</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$first</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$second</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$types</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@template T<br>@template T<br>@param Type<T> $first<br>@param Type<T> $second<br>@param Type<T> ...$types<br>@return Type<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_union(" + "$" + "{" + "1:first" + "}" + ", " + "$" + "{" + "2:second" + "}" + ", " + "$" + "{" + "3:types" + "}" + ")"),
        boost: 10
    },        {
        label: "type_uuid",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_uuid</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<Uuid>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_uuid()"),
        boost: 10
    },        {
        label: "type_xml",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_xml</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<\\DOMDocument>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_xml()"),
        boost: 10
    },        {
        label: "type_xml_element",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_xml_element</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<\\DOMElement>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_xml_element()"),
        boost: 10
    },        {
        label: "ulid",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">ulid</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string|null</span> <span class=\"fn-param\">$value</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Ulid</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\ulid(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "upper",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">upper</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToUpper</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\upper(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "uuid_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">uuid_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Uuid|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?\\Flow\\Types\\Value\\Uuid>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\uuid_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "uuid_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">uuid_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Definition<\\Flow\\Types\\Value\\Uuid>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\uuid_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "uuid_v4",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">uuid_v4</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Uuid</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\uuid_v4()"),
        boost: 10
    },        {
        label: "uuid_v7",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">uuid_v7</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|DateTimeInterface|null</span> <span class=\"fn-param\">$value</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Uuid</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\uuid_v7(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "when",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">when</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$condition</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$then</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$else</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">When</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\when(" + "$" + "{" + "1:condition" + "}" + ", " + "$" + "{" + "2:then" + "}" + ", " + "$" + "{" + "3:else" + "}" + ")"),
        boost: 10
    },        {
        label: "window",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">window</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Window</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\window()"),
        boost: 10
    },        {
        label: "with_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">with_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">WithEntry</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\with_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:function" + "}" + ")"),
        boost: 10
    },        {
        label: "write_with_retries",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">write_with_retries</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Loader</span> <span class=\"fn-param\">$loader</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">RetryStrategy</span> <span class=\"fn-param\">$retry_strategy</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Retry\\RetryStrategy\\AnyThrowable::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DelayFactory</span> <span class=\"fn-param\">$delay_factory</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Retry\\DelayFactory\\Fixed\\FixedMilliseconds::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Sleep</span> <span class=\"fn-param\">$sleep</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Time\\SystemSleep::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RetryLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\write_with_retries(" + "$" + "{" + "1:loader" + "}" + ", " + "$" + "{" + "2:retry_strategy" + "}" + ", " + "$" + "{" + "3:delay_factory" + "}" + ", " + "$" + "{" + "4:sleep" + "}" + ")"),
        boost: 10
    },        {
        label: "xml_element_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">xml_element_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DOMElement|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?\\DOMElement>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\xml_element_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "xml_element_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">xml_element_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Definition<\\DOMElement>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\xml_element_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "xml_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">xml_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DOMDocument|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?\\DOMDocument>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\xml_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },        {
        label: "xml_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">xml_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Definition<\\DOMDocument>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\xml_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    }    ]

/**
 * DSL function completion source for CodeMirror
 * @param {CompletionContext} context
 * @returns {CompletionResult|null}
 */
export function dslCompletions(context) {
    // Get text before cursor to check context
    const maxLookback = 100
    const docText = context.state.doc.toString()
    const startPos = Math.max(0, context.pos - maxLookback)
    const textBefore = docText.slice(startPos, context.pos)

    // Don't show DSL functions after -> (method chaining context)
    // DSL functions are standalone, not methods
    if (new RegExp('->\\w*$').test(textBefore)) {
        return null
    }

    // Match word being typed
    const word = context.matchBefore(/\w+/)

    // If no word and not explicit, don't show completions
    if (!word && !context.explicit) {
        return null
    }

    // Filter functions based on what's being typed
    const prefix = word ? word.text.toLowerCase() : ''
    const options = dslFunctions.filter(func =>
        !prefix || func.label.toLowerCase().startsWith(prefix)
    )

    return {
        from: word ? word.from : context.pos,
        options: options,
        validFor: new RegExp('^\\w*$')  // Reuse while typing word characters
    }
}
